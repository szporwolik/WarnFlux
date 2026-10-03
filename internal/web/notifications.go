package web

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// notificationsPerPage caps the server-rendered trail list; the page
// poller appends newer trails via the partial endpoint anyway.
const notificationsPerPage = 50

// deliveriesPerPage caps how many delivery-ledger rows the page reads
// (the details view groups them under the matching trails; the ledger
// itself is retention-bounded by PruneActionFires).
const deliveriesPerPage = 500

// notificationsView is the full /notifications page model.
type notificationsView struct {
	Lang     string
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string
	Role     string

	// FocusKey highlights one trail (?key=… deep links from the
	// dashboard warning cards).
	FocusKey string

	Trails []trailView

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavLogs          bool
	NavAudit         bool
	NavMessages      bool
	NavMeshtastic    bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
}

// trailView pairs one notification trail with the UI language for the
// notif-item sub-template (which otherwise loses the root context).
type trailView struct {
	Lang  string
	Trail trail.Trail
	// Focus opens the details block on the initial render (deep link).
	Focus bool
	// Deliveries carries the durable delivery-ledger rows of this
	// trail's event (empty when the ledger has none).
	Deliveries []deliveryRow
}

// deliveryRow is one action-fire ledger row shown in the trail details.
type deliveryRow struct {
	ActionID string
	Status   string
	Attempts int
	FiredAt  time.Time
}

// handleNotificationsPage renders the delivery history: the most recent
// notification trails, newest first. ?key= focuses one trail.
func (s *Server) handleNotificationsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	lang := s.langFor(r)
	focus := r.URL.Query().Get("key")
	view := notificationsView{
		AppTitle:         s.cfg.Title,
		Name:             s.displayName(),
		Header1:          s.displayHeader1(),
		Header2:          s.cfg.Header2,
		Tagline:          s.cfg.Tagline,
		Version:          s.version,
		Commit:           s.commit,
		RepoURL:          repoURL,
		Username:         sess.username,
		Role:             sess.role,
		CSRF:             sess.csrf,
		FocusKey:         focus,
		Trails:           wrapTrails(lang, focus, s.recentTrails(notificationsPerPage), s.recentDeliveries(deliveriesPerPage)),
		NavNotifications: true,
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "notifications", view)
}

// wrapTrails pairs each trail with a UI language and attaches the
// durable delivery rows of its event.
func wrapTrails(lang, focus string, trails []trail.Trail, deliveries map[string][]deliveryRow) []trailView {
	out := make([]trailView, 0, len(trails))
	for _, tr := range trails {
		out = append(out, trailView{
			Lang:       lang,
			Trail:      tr,
			Focus:      tr.Key == focus,
			Deliveries: deliveries[tr.Key],
		})
	}
	return out
}

// recentDeliveries reads the newest delivery-ledger rows from the
// directory store and groups them by event key. Installations without a
// SQLite directory store (or stores without the reader) yield an empty
// map — the details block then shows the empty state.
func (s *Server) recentDeliveries(limit int) map[string][]deliveryRow {
	out := make(map[string][]deliveryRow)
	ledger, ok := s.users.(interface {
		RecentDeliveries(ctx context.Context, limit int) ([]storage.DeliveryRecord, error)
	})
	if !ok {
		return out
	}
	rows, err := ledger.RecentDeliveries(context.Background(), limit)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("web: delivery ledger read failed", "error", err)
		}
		return out
	}
	for _, r := range rows {
		out[r.EventKey] = append(out[r.EventKey], deliveryRow{
			ActionID: r.ActionID,
			Status:   r.Status,
			Attempts: r.Attempts,
			FiredAt:  r.FiredAt,
		})
	}
	return out
}

// handlePartialNotifications serves the full current trail list as JSON:
// the page poller re-renders on change so new alerts appear without a
// reload. {"trails":[...]}.
func (s *Server) handlePartialNotifications(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"trails": s.recentTrails(notificationsPerPage),
	})
}

func (s *Server) recentTrails(limit int) []trail.Trail {
	if s.trails == nil {
		return []trail.Trail{}
	}
	trails := s.trails.Recent(limit)
	if trails == nil {
		trails = []trail.Trail{}
	}
	return trails
}
