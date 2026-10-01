package sqlite

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestDeliveryClaimIndexes pins the v28 scheduler indexes: every action
// worker polls the claim query every 250 ms and the manager sweeps the
// recovery query every second — without the indexes both scan the whole
// action_fires history, so their cost would grow with the ledger.
func TestDeliveryClaimIndexes(t *testing.T) {
	s, _, err := Open(filepath.Join(t.TempDir(), "idx.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name IN ('idx_action_fires_claim', 'idx_action_fires_recover')`).Scan(&n); err != nil {
		t.Fatalf("read index catalog: %v", err)
	}
	if n != 2 {
		t.Fatalf("delivery scheduler indexes missing (%d of 2 found)", n)
	}

	// The claim query must be served by idx_action_fires_claim.
	plan := queryPlan(t, s,
		`SELECT group_id, action_id, dedup_key FROM action_fires
		 WHERE action_id = 'log' AND status IN ('saved','failed')
		   AND next_attempt_at_ms <= 0 AND (attempts < 3 OR status = 'saved')
		 ORDER BY next_attempt_at_ms ASC, fired_at_ms ASC LIMIT 1`)
	if !strings.Contains(plan, "idx_action_fires_claim") {
		t.Errorf("claim query does not use idx_action_fires_claim: %s", plan)
	}

	// The recovery sweep must be served by idx_action_fires_recover.
	plan = queryPlan(t, s,
		`UPDATE action_fires SET status = 'saved'
		 WHERE status = 'running' AND next_attempt_at_ms != 0 AND next_attempt_at_ms < 1`)
	if !strings.Contains(plan, "idx_action_fires_recover") {
		t.Errorf("recovery query does not use idx_action_fires_recover: %s", plan)
	}
}

// queryPlan returns the EXPLAIN QUERY PLAN detail lines for one
// statement.
func queryPlan(t *testing.T, s *Store, query string) string {
	t.Helper()
	rows, err := s.db.Query("EXPLAIN QUERY PLAN " + query)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		lines = append(lines, detail)
	}
	return strings.Join(lines, "\n")
}
