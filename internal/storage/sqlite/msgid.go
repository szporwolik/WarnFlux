package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// Why the mapping also lives in memory:
//
// Message ids are read from inside store transactions. The ingest flow
// writes an event's inbox row while its transaction is open, and the
// payload carries the id (dispatch.EventForJournalChange). With the
// single pooled connection Open installs, a database lookup there waits
// for the very connection the caller holds — a self-deadlock that parked
// an ingest transaction in LoadMessageID and wedged every later request
// until the process was restarted. The same holds for the insert: a new
// assignment written from inside a transaction would wait for the
// transaction itself.
//
// The store therefore serves lookups from memory — loaded once at Open,
// updated on every assignment and on every prune — and hands new
// assignments to a single background writer. MessageID's store seam
// never touches the database from the caller's goroutine, so no caller
// can block on its own transaction, and the durable rows still land.
//
// The in-memory copy is authoritative for lookups: PruneMessageIDs drops
// exactly the keys its DELETE reported, and an assignment is immutable
// once recorded, so a key absent from the copy was never assigned.
//
// A writer behind the caller owes it durability: the assignment is
// reported as recorded, and operators cite the number on the air. A batch
// the database refuses is therefore retried and kept for as long as the
// station runs (only shutdown, which cannot wait forever, gives up), so a
// transient write failure delays the row instead of losing it.

// messageIDMinuteKeep bounds the in-memory per-minute sequence buckets.
// The sequence is only ever asked about the current minute, so buckets
// beyond this window are noise; keeping them would grow without bound.
const messageIDMinuteKeep = 48 * time.Hour

// initMessageIDs loads the durable assignments into memory and starts
// the writer that persists new ones.
func (s *Store) initMessageIDs(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_key, msg_id, minute, seq FROM message_ids`)
	if err != nil {
		return fmt.Errorf("load message ids: %w", err)
	}
	defer rows.Close()

	byKey := make(map[string]string)
	seqIn := make(map[string]int)
	for rows.Next() {
		var key, id, minute string
		var seq int
		if err := rows.Scan(&key, &id, &minute, &seq); err != nil {
			return fmt.Errorf("scan message id: %w", err)
		}
		byKey[key] = id
		if seq > seqIn[minute] {
			seqIn[minute] = seq
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load message ids: %w", err)
	}

	wake := make(chan struct{}, 1)
	stop := make(chan struct{})

	s.msgMu.Lock()
	s.msgByKey, s.msgSeqIn = byKey, seqIn
	s.msgWake, s.msgStop = wake, stop
	s.msgMu.Unlock()
	s.msgWG.Add(1)
	go s.messageIDWriter(wake, stop)
	return nil
}

// LoadMessageID returns the persisted short message id of eventKey;
// ok=false when the key was never assigned. It answers from memory and
// never touches the database, so it is safe to call with a transaction
// open — where a query would deadlock on the single pooled connection.
func (s *Store) LoadMessageID(_ context.Context, eventKey string) (string, bool, error) {
	s.msgMu.Lock()
	id, ok := s.msgByKey[eventKey]
	s.msgMu.Unlock()
	return id, ok, nil
}

// HighestSeqInMinute returns the largest sequence already handed out in
// the given minute bucket (0 when none). It lets a restarted process
// resume the per-minute sequence instead of reusing a number. Like
// LoadMessageID it answers from memory.
func (s *Store) HighestSeqInMinute(_ context.Context, minute string) (int, error) {
	s.msgMu.Lock()
	n := s.msgSeqIn[minute]
	s.msgMu.Unlock()
	return n, nil
}

// SaveMessageID records one event-key → message-id assignment. The
// mapping is immutable: the first writer wins, so a replayed assignment
// never renumbers an already-cited event. The record is queued for the
// background writer and visible to LoadMessageID immediately, which is
// what makes it safe to call from inside a transaction.
//
// The queue is the record of assignments that are in memory but not yet
// in message_ids. It is never dropped while the station runs (see
// messageIDWriter), so "assigned in memory" always implies "still queued
// or already written": a replayed assignment needs no repair.
func (s *Store) SaveMessageID(_ context.Context, a core.MessageIDAssignment) error {
	s.msgMu.Lock()
	if _, exists := s.msgByKey[a.EventKey]; exists {
		s.msgMu.Unlock()
		return nil
	}
	s.msgByKey[a.EventKey] = a.MsgID
	if a.Seq > s.msgSeqIn[a.Minute] {
		s.msgSeqIn[a.Minute] = a.Seq
	}
	wake := s.msgWake
	if wake != nil {
		s.msgQueue = append(s.msgQueue, a)
	}
	s.msgMu.Unlock()

	wakeMessageIDWriter(wake)
	return nil
}

// PruneMessageIDs deletes assignments whose event no longer exists (in
// neither the current-state events nor the panel compose hazards) and
// that are older than olderThan. Ids of retained events are never
// touched, so a cited number cannot change while its event is kept.
func (s *Store) PruneMessageIDs(ctx context.Context, olderThan time.Time) (int64, error) {
	// Queued assignments land first: the DELETE must judge the rows that
	// exist, and a key deleted here is dropped from the mirror below.
	if err := s.flushMessageIDs(); err != nil && s.msgLogf != nil {
		s.msgLogf("message id write failed", "error", err)
	}
	rows, err := s.db.QueryContext(ctx, `
		DELETE FROM message_ids
		WHERE created_at_ms < ?
		  AND event_key NOT IN (SELECT event_key FROM events)
		  AND event_key NOT IN (SELECT event_key FROM compose_hazards)
		RETURNING event_key`, olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune message ids: %w", err)
	}
	var pruned []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return 0, fmt.Errorf("prune message ids: %w", err)
		}
		pruned = append(pruned, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, fmt.Errorf("prune message ids: %w", err)
	}

	s.msgMu.Lock()
	for _, key := range pruned {
		delete(s.msgByKey, key)
	}
	// Pruned rows may leave a minute bucket with no assignments behind;
	// the buckets only matter for the minute currently in progress.
	cutoff := s.now().UTC().Add(-messageIDMinuteKeep).Format("20060102-1504")
	for minute := range s.msgSeqIn {
		if minute < cutoff {
			delete(s.msgSeqIn, minute)
		}
	}
	s.msgMu.Unlock()
	return int64(len(pruned)), nil
}

// messageIDWriter persists queued assignments. It runs alone, so its
// inserts serialize behind (never inside) an open transaction: waiting
// for the connection there cannot block the caller that queued the row.
// Both channels are captured here: clearing the store fields on shutdown
// must not leave this loop selecting on nil.
//
// A batch the database refuses is retried on a growing delay and KEPT.
// Giving up would be a silent renumbering: the id is already assigned in
// memory (and may already be on the air), so the row it never wrote is
// the only thing linking the number to the event — a restart would hand
// that event a different one. Only shutdown gives up, on its own bound
// (see finalFlushMessageIDs).
func (s *Store) messageIDWriter(wake, stop <-chan struct{}) {
	defer s.msgWG.Done()
	attempt := 0
	for {
		select {
		case <-stop:
			s.finalFlushMessageIDs()
			return
		case <-wake:
		}
		for {
			err := s.flushMessageIDs()
			if err == nil {
				attempt = 0
				break
			}
			delay := s.messageIDRetryDelay(attempt)
			attempt++
			if s.msgLogf != nil {
				s.msgLogf("message id write failed", "error", err, "attempt", attempt, "retry_in", delay)
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-stop:
				timer.Stop()
				s.finalFlushMessageIDs()
				return
			}
		}
	}
}

// messageIDRetrySchedule is the delay the writer waits after a failed
// write, growing with every attempt; its last step repeats for as long as
// the station runs, because the queue is never dropped while it does.
var messageIDRetrySchedule = []time.Duration{
	100 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	15 * time.Second,
}

// messageIDRetryDelay returns the delay after the given zero-based
// failed attempt, from the store's schedule.
func (s *Store) messageIDRetryDelay(attempt int) time.Duration {
	sched := s.msgRetry
	if len(sched) == 0 {
		sched = messageIDRetrySchedule
	}
	if attempt >= len(sched) {
		attempt = len(sched) - 1
	}
	return sched[attempt]
}

// flushMessageIDs attempts one pass over the queued assignments and
// leaves a failed batch queued for the next attempt. The nudge on the
// failure path covers the callers outside the writer (PruneMessageIDs):
// a batch re-queued by them must not wait for the next assignment before
// anyone retries it.
func (s *Store) flushMessageIDs() error {
	s.msgMu.Lock()
	if len(s.msgQueue) == 0 {
		s.msgMu.Unlock()
		return nil
	}
	batch := s.msgQueue
	s.msgQueue = nil
	s.msgMu.Unlock()

	if err := s.writeMessageIDs(batch); err != nil {
		s.msgMu.Lock()
		s.msgQueue = append(batch, s.msgQueue...)
		wake := s.msgWake
		s.msgMu.Unlock()
		wakeMessageIDWriter(wake)
		return err
	}
	return nil
}

// wakeMessageIDWriter nudges the writer without blocking: the wake
// channel holds one token, so a burst of assignments collapses into one
// drain (which takes the whole queue anyway).
func wakeMessageIDWriter(wake chan struct{}) {
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

// writeMessageIDs inserts one batch of assignments; an existing key is
// left alone because the first assignment is the one already cited.
func (s *Store) writeMessageIDs(batch []core.MessageIDAssignment) error {
	for _, a := range batch {
		if _, err := s.db.ExecContext(context.Background(), `
			INSERT INTO message_ids(event_key, msg_id, minute, seq, created_at_ms)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(event_key) DO NOTHING`,
			a.EventKey, a.MsgID, a.Minute, a.Seq, a.CreatedAt.UTC().UnixMilli()); err != nil {
			return fmt.Errorf("save message id for %q: %w", a.EventKey, err)
		}
	}
	return nil
}

// messageIDShutdownWait bounds how long Close waits for the final flush:
// a database that no longer accepts writes must not turn a shutdown into
// a hang.
const messageIDShutdownWait = 5 * time.Second

// messageIDShutdownFlush bounds the last-chance write attempt inside that
// wait. It is deliberately NOT the runtime schedule: while the station
// runs the queue is kept forever, but a process that is exiting cannot
// wait for the database to come back, so the writer has to be finished
// before Close stops waiting for it.
const messageIDShutdownFlush = 3 * time.Second

// finalFlushMessageIDs is the last chance to persist the queue before the
// process exits: bounded retries inside messageIDShutdownFlush, then a
// loud give-up naming the assignments that were lost.
func (s *Store) finalFlushMessageIDs() {
	deadline := time.Now().Add(messageIDShutdownFlush)
	for {
		err := s.flushMessageIDs()
		if err == nil {
			return
		}
		left := time.Until(deadline)
		if left <= 0 {
			s.msgMu.Lock()
			pending := len(s.msgQueue)
			s.msgQueue = nil
			s.msgMu.Unlock()
			if pending > 0 && s.msgLogf != nil {
				s.msgLogf("message id assignments abandoned at shutdown",
					"pending", pending, "error", err)
			}
			return
		}
		if s.msgLogf != nil {
			s.msgLogf("message id write failed", "error", err, "shutdown_flush_left", left)
		}
		delay := s.messageIDRetryDelay(0)
		if delay > left {
			delay = left
		}
		time.Sleep(delay)
	}
}

// stopMessageIDs drains the queue and stops the writer; called by Close
// so a shutting-down process never loses an assignment it reported.
func (s *Store) stopMessageIDs() {
	s.msgMu.Lock()
	stop := s.msgStop
	s.msgStop, s.msgWake = nil, nil
	s.msgMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)

	done := make(chan struct{})
	go func() {
		s.msgWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(messageIDShutdownWait):
		if s.msgLogf != nil {
			s.msgLogf("message id writer did not stop in time")
		}
	}
}

var _ storage.MessageIDStore = (*Store)(nil)
