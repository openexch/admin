// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/services"
)

// recordingRunner captures the per-node fan-out so the handler test never spawns
// java. It returns a canned output and, optionally, an error for a chosen node.
type recordingRunner struct {
	mu    sync.Mutex
	calls []struct {
		node int
		root string
		seq  int64
	}
	failNode int // -1 = never fail
}

func (rr *recordingRunner) run(c *services.Cluster, node int, root string, seq int64) (string, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.calls = append(rr.calls, struct {
		node int
		root string
		seq  int64
	}{node, root, seq})
	if node == rr.failNode {
		return "boom on node " + fmt.Sprint(node), fmt.Errorf("exit status 1")
	}
	return fmt.Sprintf("[JOURNAL-RETENTION] node %d ok (seq=%d)", node, seq), nil
}

func newJournalRetentionServer(t *testing.T, journalDir string, runner *recordingRunner) *httptest.Server {
	t.Helper()
	cfg := &config.Config{SettlementJournalDir: journalDir}
	h := &Handlers{
		cfg:           cfg,
		cluster:       services.NewMatchCluster(cfg), // default 3 nodes
		journalRunner: runner.run,
	}
	r := chi.NewRouter()
	r.Post("/api/admin/journal-retention", h.handleJournalRetention)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func postRetention(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/api/admin/journal-retention", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestJournalRetention_RejectsNonPositiveWatermark(t *testing.T) {
	runner := &recordingRunner{failNode: -1}
	srv := newJournalRetentionServer(t, "/journal", runner)

	for _, body := range []string{`{"safeEgressSeq":0}`, `{"safeEgressSeq":-5}`, `{}`} {
		resp := postRetention(t, srv.URL, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q: expected 400, got %d", body, resp.StatusCode)
		}
		resp.Body.Close()
	}
	if len(runner.calls) != 0 {
		t.Fatalf("no node should be touched for a non-positive watermark, got %d calls", len(runner.calls))
	}
}

func TestJournalRetention_ConflictWhenJournalUnconfigured(t *testing.T) {
	runner := &recordingRunner{failNode: -1}
	srv := newJournalRetentionServer(t, "", runner) // SETTLEMENT_JOURNAL_DIR unset -> feature off

	resp := postRetention(t, srv.URL, `{"safeEgressSeq":100}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 when journal unconfigured, got %d", resp.StatusCode)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("no node should be touched when unconfigured, got %d calls", len(runner.calls))
	}
}

func TestJournalRetention_FansOutPerNodeAndAggregates(t *testing.T) {
	runner := &recordingRunner{failNode: -1}
	srv := newJournalRetentionServer(t, "/journal", runner)

	resp := postRetention(t, srv.URL, `{"safeEgressSeq":4242}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out struct {
		SafeEgressSeq int64                    `json:"safeEgressSeq"`
		Failures      int                      `json:"failures"`
		Results       []map[string]interface{} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.SafeEgressSeq != 4242 {
		t.Fatalf("watermark echoed wrong: %d", out.SafeEgressSeq)
	}
	if out.Failures != 0 {
		t.Fatalf("expected 0 failures, got %d", out.Failures)
	}
	if len(out.Results) != 3 {
		t.Fatalf("expected 3 per-node results (default node count), got %d", len(out.Results))
	}

	// Every node was invoked exactly once, in order, with the configured root + watermark.
	if len(runner.calls) != 3 {
		t.Fatalf("expected 3 runner calls, got %d", len(runner.calls))
	}
	for i, c := range runner.calls {
		if c.node != i || c.root != "/journal" || c.seq != 4242 {
			t.Fatalf("call %d wrong: %+v", i, c)
		}
	}
}

func TestJournalRetention_SurfacesPerNodeFailure(t *testing.T) {
	runner := &recordingRunner{failNode: 1} // node 1 fails
	srv := newJournalRetentionServer(t, "/journal", runner)

	resp := postRetention(t, srv.URL, `{"safeEgressSeq":7}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 (partial failure still reports per-node), got %d", resp.StatusCode)
	}

	var out struct {
		Failures int                      `json:"failures"`
		Results  []map[string]interface{} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Failures != 1 {
		t.Fatalf("expected 1 failure, got %d", out.Failures)
	}
	// The failing node carries an "error" field; the healthy ones do not.
	if _, ok := out.Results[1]["error"]; !ok {
		t.Fatalf("node 1 result should carry an error: %+v", out.Results[1])
	}
	if _, ok := out.Results[0]["error"]; ok {
		t.Fatalf("node 0 should have no error: %+v", out.Results[0])
	}
}
