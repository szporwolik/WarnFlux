package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// meshOutboxFlushEvery is how often the queue is drained while the radio is up.
// A message queued during a reconnect wait therefore leaves within seconds of the
// session coming back, and the interval costs one store query when it does not.
const meshOutboxFlushEvery = 10 * time.Second

// meshOutboxSendBudget bounds one delivery attempt: the same 15 s the panel's
// own sends get, so a dead link cannot stall the drain loop.
const meshOutboxSendBudget = 15 * time.Second

// meshOutboxView is one row of the panel's queue list.
type meshOutboxView struct {
	ID       int64
	Kind     string
	Label    string
	Text     string
	Preview  string
	Operator string
	Created  string
	Attempts int
	Error    string
	Sent     bool
	SentAt   string
	// Status is the localised state: "waiting", "sent" or "abandoned".
	Status string
	State  string
}

// enqueueMeshOutbox keeps a message the radio could not take. kind is "dm" or
// "channel".
func (s *Server) enqueueMeshOutbox(ctx context.Context, item storage.MeshOutboxItem) error {
	if s.meshOutbox == nil {
		return errors.New("web: no outbox store configured")
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	_, err := s.meshOutbox.EnqueueMeshOutbox(ctx, item)
	return err
}

// isTransientMeshSend reports whether a failed send may be retried later: the
// message never left the station because the device was between sessions. A bad
// recipient or a refused channel is the operator's problem and must not sit in
// the queue forever.
func isTransientMeshSend(err error) bool {
	return err != nil && errors.Is(err, mesh.ErrDeviceNotConnected)
}

// RunMeshOutbox drains the queue until ctx is cancelled. It is started once at
// boot; without a store or a hub it returns at once.
func (s *Server) RunMeshOutbox(ctx context.Context) {
	if s.meshOutbox == nil || s.meshtastic == nil {
		return
	}
	ticker := time.NewTicker(meshOutboxFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.flushMeshOutbox(ctx)
		}
	}
}

// meshSender is the part of the radio hub the queue needs. It exists so the
// dispatcher can be driven by a stub: a queue that only runs against a real
// device is a queue nobody tests.
type meshSender interface {
	Connected() bool
	SendContactMessage(ctx context.Context, addr, text, operator string) error
	SendChannelText(ctx context.Context, idx int, text, operator string) error
}

// flushMeshOutbox sends whatever is waiting, oldest first, and stops at the
// first transient failure: the link is gone again and the next tick continues.
func (s *Server) flushMeshOutbox(ctx context.Context) {
	if s.meshOutbox == nil || s.meshtastic == nil {
		return
	}
	s.flushMeshOutboxWith(ctx, s.meshtastic, s.meshOutbox)
}

// flushMeshOutboxWith is the dispatcher proper.
func (s *Server) flushMeshOutboxWith(ctx context.Context, sender meshSender, store storage.MeshOutboxStore) {
	if sender == nil || store == nil || !sender.Connected() {
		return
	}
	items, err := store.PendingMeshOutbox(ctx, 5)
	if err != nil {
		s.logger.Warn("mesh outbox: pending read failed", "error", err)
		return
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return
		}
		sendCtx, cancel := context.WithTimeout(ctx, meshOutboxSendBudget)
		var sendErr error
		if item.Kind == "channel" {
			sendErr = sender.SendChannelText(sendCtx, item.ChannelIdx, item.Text, item.Operator)
		} else {
			sendErr = sender.SendContactMessage(sendCtx, item.TargetID, item.Text, item.Operator)
		}
		cancel()
		if sendErr == nil {
			if err := store.MarkMeshOutboxSent(ctx, item.ID, time.Now()); err != nil {
				s.logger.Warn("mesh outbox: mark sent failed", "id", item.ID, "error", err)
			}
			s.logger.Info("mesh outbox: queued message sent", "id", item.ID, "kind", item.Kind, "target", item.Label,
				"waiting", time.Since(item.CreatedAt).Truncate(time.Second).String())
			continue
		}
		if err := store.MarkMeshOutboxAttempt(ctx, item.ID, sendErr.Error()); err != nil {
			s.logger.Warn("mesh outbox: mark attempt failed", "id", item.ID, "error", err)
		}
		if isTransientMeshSend(sendErr) {
			return
		}
		s.logger.Warn("mesh outbox: queued message rejected", "id", item.ID, "target", item.Label, "error", sendErr)
	}
	// The queue is drained (or the link died): drop what nobody needs to see.
	if err := store.PruneMeshOutbox(ctx, time.Now().Add(-storage.MeshOutboxRetention)); err != nil {
		s.logger.Warn("mesh outbox: prune failed", "error", err)
	}
}

// meshOutboxPreview shortens the message for the list: the queue is a
// confirmation, not a reader.
func meshOutboxPreview(text string, max int) string {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	if len([]rune(text)) <= max {
		return text
	}
	return string([]rune(text)[:max]) + "…"
}

// fillMeshOutbox loads the queue for the panel, newest first.
func (s *Server) fillMeshOutbox(ctx context.Context, v *meshtasticView) {
	if s.meshOutbox == nil {
		return
	}
	items, err := s.meshOutbox.RecentMeshOutbox(ctx, 12)
	if err != nil {
		s.logger.Warn("mesh outbox: panel read failed", "error", err)
		return
	}
	loc := s.displayLoc()
	out := make([]meshOutboxView, 0, len(items))
	for _, item := range items {
		if item.SentAt.IsZero() && item.Attempts >= storage.MeshOutboxMaxAttempts {
			continue // abandoned and already swept by the prune
		}
		row := meshOutboxView{
			ID:       item.ID,
			Kind:     item.Kind,
			Label:    item.Label,
			Text:     item.Text,
			Preview:  meshOutboxPreview(item.Text, 80),
			Operator: item.Operator,
			Created:  item.CreatedAt.In(loc).Format("02.01 15:04"),
			Attempts: item.Attempts,
			Error:    item.LastError,
			Sent:     !item.SentAt.IsZero(),
		}
		switch {
		case !item.SentAt.IsZero():
			row.State = "sent"
			row.SentAt = item.SentAt.In(loc).Format("15:04")
		case item.Attempts >= storage.MeshOutboxMaxAttempts:
			row.State = "abandoned"
		default:
			row.State = "waiting"
		}
		out = append(out, row)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	v.Outbox = out
	v.OutboxWaiting = 0
	for _, row := range out {
		if row.State == "waiting" {
			v.OutboxWaiting++
		}
	}
}

// meshOutboxStatusText is the localised state label.
func meshOutboxStatusText(lang, state string) string {
	switch state {
	case "sent":
		return i18n.T(lang, "meshtastic.outbox_sent")
	case "abandoned":
		return i18n.T(lang, "meshtastic.outbox_abandoned")
	default:
		return i18n.T(lang, "meshtastic.outbox_waiting")
	}
}

// handleMeshOutboxCancel drops one queued message the operator no longer wants.
func (s *Server) handleMeshOutboxCancel(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(strings.TrimSpace(r.PostFormValue("id")), 10, 64)
	if err != nil || id <= 0 {
		http.Redirect(w, r, "/meshtastic?tab=msgs&err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.outbox_bad_id")), http.StatusSeeOther)
		return
	}
	if s.meshOutbox != nil {
		if err := s.meshOutbox.DeleteMeshOutbox(r.Context(), id); err != nil {
			s.logger.Warn("mesh outbox: delete failed", "id", id, "error", err)
		} else {
			s.audit(sess.username, "meshtastic-outbox-cancel", fmt.Sprintf("id=%d", id))
		}
	}
	http.Redirect(w, r, "/meshtastic?tab=msgs", http.StatusSeeOther)
}

// meshOutboxLabel names a queued direct message the way the operator picked the
// recipient: the station's own directory name for the node, else the node id.
func (s *Server) meshOutboxLabel(id string) string {
	if s.meshtastic != nil {
		if label := s.meshtastic.NodeLabel(id); label != "" {
			return label
		}
	}
	if id == "" {
		return ""
	}
	return "!" + id
}
