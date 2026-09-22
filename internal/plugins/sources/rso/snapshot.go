package rso

import (
	"context"
	"encoding/xml"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// feedResult is one regional fetch outcome.
type feedResult struct {
	voivodeship string
	items       []newsItem
	complete    bool // fetch OK, XML well formed, structure consistent
}

// pollOnce fetches every configured voivodeship, builds ONE combined
// logical snapshot and ingests it. Source health is healthy only when the
// combined snapshot is complete; a failed regional query never looks like
// "all warnings from that region disappeared".
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter, reporter plugin.SourceHealthReporter) {
	now := time.Now()
	results := make([]feedResult, 0, len(s.cfg.Voivodeships))
	healthy := true
	for _, v := range s.cfg.Voivodeships {
		if ctx.Err() != nil {
			return // shutting down: no health report for a cancelled poll
		}
		fr := s.fetchFeed(ctx, v)
		if !fr.complete {
			healthy = false
		}
		results = append(results, fr)
	}
	if ctx.Err() != nil {
		return
	}

	keys, complete := s.ingestCombined(ctx, emit, results)
	if !complete {
		healthy = false
	} else {
		cancelled, err := snapshotutil.Reconcile(ctx, emit, sourceRSO, keys, now)
		if err != nil {
			slog.Warn("RSO snapshot reconciliation failed", "error", err)
			healthy = false
		} else {
			slog.Debug("RSO snapshot processed", "voivodeships", len(results), "items", len(keys), "cancelled", cancelled)
		}
	}

	if reporter != nil {
		if healthy {
			reporter.ReportSourceHealthy()
		} else {
			reporter.ReportSourceDegraded(errors.New("one or more RSO regional feeds failed or the combined snapshot was incomplete"))
		}
	}
}

// fetchFeed fetches and structurally validates one regional feed. A HTTP
// 200 alone is never a valid snapshot: the body must be well-formed XML
// with the expected <newses> root and consistent pagination metadata.
func (s *Source) fetchFeed(ctx context.Context, voivodeship string) feedResult {
	body, err := s.client.Fetch(ctx, voivodeship)
	if err != nil {
		if ctx.Err() != nil {
			return feedResult{voivodeship: voivodeship}
		}
		slog.Warn("RSO feed fetch failed", "voivodeship", voivodeship, "error", err)
		return feedResult{voivodeship: voivodeship}
	}

	var list newsList
	if err := xml.Unmarshal(body, &list); err != nil {
		// Malformed XML, wrong root element (e.g. an HTML error page) or
		// truncated body: provider failure, never an empty snapshot.
		slog.Warn("RSO feed is not valid RSO XML", "voivodeship", voivodeship, "error", err)
		return feedResult{voivodeship: voivodeship}
	}
	if list.PaginationInfo.TotalItems != len(list.News) {
		slog.Warn("RSO feed pagination metadata does not match the item count",
			"voivodeship", voivodeship, "total_items", list.PaginationInfo.TotalItems, "items", len(list.News))
		return feedResult{voivodeship: voivodeship, items: list.News}
	}
	if len(list.News) > maxItemsPerFeed {
		slog.Warn("RSO feed exceeds the item bound", "voivodeship", voivodeship, "items", len(list.News), "maximum", maxItemsPerFeed)
		return feedResult{voivodeship: voivodeship}
	}
	return feedResult{voivodeship: voivodeship, items: list.News, complete: true}
}

// ingestCombined builds ONE logical snapshot from all configured regional
// feeds in TWO PHASES. Phase 1 normalizes every item and merges the same
// provider ID across regions (union of areas; conflicting non-area content
// marks the identity ambiguous and the combined snapshot incomplete).
// Phase 2 emits only unambiguous identities. Disappearance reconciliation
// (done by the caller) requires the combined snapshot to be complete.
func (s *Source) ingestCombined(ctx context.Context, emit plugin.Emitter, results []feedResult) (map[string]bool, bool) {
	type candidate struct {
		ev        core.HazardEvent
		signature string
		conflict  bool
	}
	candidates := make(map[string]*candidate)
	complete := true

	for _, fr := range results {
		if !fr.complete {
			complete = false
		}
		for _, item := range fr.items {
			if ctx.Err() != nil {
				return nil, false
			}
			ev, err := normalizeNews(item, listURL(s.cfg.BaseURL, fr.voivodeship), fr.voivodeship)
			if err != nil {
				slog.Warn("RSO item skipped; combined snapshot marked incomplete",
					"voivodeship", fr.voivodeship, "id", strings.TrimSpace(item.ID), "error", err)
				complete = false
				continue
			}
			sig := contentSignature(item)
			if c, ok := candidates[ev.Key()]; ok {
				if c.signature != sig {
					// Same provider ID with conflicting content across
					// regional feeds: ambiguous, never emitted.
					c.conflict = true
					complete = false
					slog.Warn("RSO duplicate identity has conflicting content; not emitted",
						"event_key", ev.Key())
					continue
				}
				c.ev.Areas = sortedUnique(append(c.ev.Areas, ev.Areas...))
				continue
			}
			candidates[ev.Key()] = &candidate{ev: ev, signature: sig}
		}
	}

	keys := make(map[string]bool, len(candidates))
	for key, c := range candidates {
		if ctx.Err() != nil {
			return keys, false
		}
		if c.conflict {
			continue
		}
		keys[key] = true
		if err := emit.Emit(ctx, c.ev); err != nil {
			if ctx.Err() != nil {
				return keys, false
			}
			slog.Warn("emit failed; retrying on the next poll", "event_key", key, "error", err)
			continue
		}
	}
	return keys, complete
}

// sortedUnique returns the deduplicated, lexicographically sorted list.
func sortedUnique(in []string) []string {
	sort.Strings(in)
	out := in[:0]
	var prev string
	for i, s := range in {
		if i > 0 && s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}
