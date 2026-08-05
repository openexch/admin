// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/openexch/admin/agent"
)

// MetricsService renders Prometheus text exposition (companion to the match
// nodes' NodeMetricsServer and the OMS Micrometer endpoint — admin-gateway#12).
// Zero dependencies by design: everything is read at scrape time from state
// the gateway already maintains (the 2s status cache, live PM info, the
// backup heartbeat file, the progress slot), so a scrape never spawns a JVM
// or touches the cluster.
type MetricsService struct {
	statusSvc *StatusService
	opsSvc    *OperationsService
	pm        agent.ProcessAgent
	progress  *Progress
	preflight *Preflight
	startTime time.Time

	mu       sync.Mutex
	requests map[string]int64 // "method|route|code" -> count
}

func NewMetricsService(statusSvc *StatusService, opsSvc *OperationsService, pm agent.ProcessAgent, progress *Progress, preflight *Preflight) *MetricsService {
	return &MetricsService{
		statusSvc: statusSvc,
		opsSvc:    opsSvc,
		pm:        pm,
		progress:  progress,
		preflight: preflight,
		startTime: time.Now(),
		requests:  make(map[string]int64),
	}
}

// Middleware counts requests per chi route pattern (fixed cardinality: the
// pattern is the registered template, never the raw URL) and status code.
func (m *MetricsService) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		key := r.Method + "|" + route + "|" + fmt.Sprintf("%d", sw.status)
		m.mu.Lock()
		m.requests[key]++
		m.mu.Unlock()
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer's
// Flush/deadline support through this wrapper (the SSE events stream flushes
// per frame; without this the wrapper hid http.Flusher and /api/admin/events
// returned "streaming unsupported" behind the metrics middleware).
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Flush forwards to the underlying writer for direct http.Flusher asserts.
func (w *statusWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// Handler serves GET /metrics.
func (m *MetricsService) Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(m.render()))
}

func (m *MetricsService) render() string {
	var b strings.Builder
	b.Grow(8192)

	gauge := func(name, help string, value float64, labels string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		if labels != "" {
			fmt.Fprintf(&b, "%s{%s} %g\n", name, labels, value)
		} else {
			fmt.Fprintf(&b, "%s %g\n", name, value)
		}
	}
	// series appends an additional sample to an already-declared metric
	series := func(name, labels string, value float64) {
		fmt.Fprintf(&b, "%s{%s} %g\n", name, labels, value)
	}
	head := func(name, help, typ string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}

	gauge("admin_uptime_seconds", "Seconds since the admin gateway started.",
		time.Since(m.startTime).Seconds(), "")

	// ---- cluster (from the 2s status cache) ----
	status := m.statusSvc.GetStatus()

	gauge("admin_cluster_leader", "Leader node id (-1 when unknown).",
		toF(status["leader"], -1), "")
	gauge("admin_cluster_all_nodes_healthy", "1 when all three nodes are HEALTHY.",
		boolF(status["allNodesHealthy"]), "")
	gauge("admin_demo_healthy", "1 when the market simulator's end-to-end demo canary is healthy (orders, fills, market data, CORS incl. public edge).",
		boolF(status["demoHealthy"]), "")

	// Active runtime profile as a label'd gauge=1 (the series' label carries the
	// value, à la node_exporter's info metrics), so a dashboard can alert on a
	// profile drift or show which profile the stack is running.
	if prof, ok := status["activeProfile"].(string); ok && prof != "" {
		gauge("admin_active_profile", "The active stack runtime profile (label carries the name; value always 1).",
			1, fmt.Sprintf("profile=%q", prof))
	}

	if nodes, ok := status["nodes"].([]map[string]interface{}); ok {
		head("admin_node_healthy", "1 when the node's derived health is HEALTHY.", "gauge")
		for _, n := range nodes {
			series("admin_node_healthy", nodeLabel(n), boolS(n["health"], HealthHealthy))
		}
		head("admin_node_commit_advancing", "1 when the node's commit position advanced last poll.", "gauge")
		for _, n := range nodes {
			series("admin_node_commit_advancing", nodeLabel(n), boolF(n["commitAdvancing"]))
		}
		head("admin_node_commit_position", "Raft commit position from the node's CnC counters.", "gauge")
		for _, n := range nodes {
			if v, ok := n["commitPosition"]; ok {
				series("admin_node_commit_position", nodeLabel(n), toF(v, 0))
			}
		}
		head("admin_node_snapshot_count", "Snapshots taken according to CnC counters.", "gauge")
		for _, n := range nodes {
			if v, ok := n["snapshotCount"]; ok {
				series("admin_node_snapshot_count", nodeLabel(n), toF(v, 0))
			}
		}
		head("admin_node_archive_bytes", "Archive segment bytes on the node's tmpfs.", "gauge")
		for _, n := range nodes {
			if v, ok := n["archiveBytes"]; ok {
				series("admin_node_archive_bytes", nodeLabel(n), toF(v, 0))
			}
		}
	}

	// ---- pre-flight invariants (from the 2s status cache; #42/#43/#45) ----
	if inv, ok := status["invariants"].([]InvariantResult); ok {
		head("admin_invariant_ok", "1 when the named pre-flight invariant holds (see GET /api/admin/preflight).", "gauge")
		for _, r := range inv {
			series("admin_invariant_ok", fmt.Sprintf("check=%q", r.Name), b2f(r.OK))
		}
	}
	if m.preflight != nil {
		if avail := m.preflight.MemAvailableBytes(); avail >= 0 {
			gauge("admin_mem_available_bytes", "Host MemAvailable from /proc/meminfo (the #43 pre-flight input).",
				float64(avail), "")
		}
	}

	if gws, ok := status["gateways"].(map[string]interface{}); ok {
		head("admin_gateway_healthy", "1 when the service's health probe returns 200.", "gauge")
		for _, name := range []string{"market", "oms"} {
			if gw, ok := gws[name].(map[string]interface{}); ok {
				series("admin_gateway_healthy", fmt.Sprintf("service=%q", name), boolF(gw["healthy"]))
			}
		}
	}

	// ---- backup freshness (heartbeat file; match#36) ----
	info := m.opsSvc.GetBackupInfo()
	gauge("admin_backup_fresh", "1 when the backup heartbeat is recent and in state OK (trust this, never running alone).",
		b2f(info.Fresh), "")
	gauge("admin_backup_recording_log_bytes", "Size of the on-disk backup recording log.",
		float64(info.RecordingLogBytes), "")
	if hb := info.Heartbeat; hb != nil {
		gauge("admin_backup_heartbeat_age_seconds", "Age of the backup app's heartbeat file.",
			time.Since(time.UnixMilli(hb.UpdatedEpochMs)).Seconds(), "")
		gauge("admin_backup_live_log_position", "Live log position replicated to disk.",
			float64(hb.LiveLogPosition), "")
		gauge("admin_backup_snapshots_retrieved_total", "Snapshots retrieved by the backup app since it started.",
			float64(hb.SnapshotsRetrieved), "")
		gauge("admin_backup_stall_warnings_total", "Stall warnings logged by the backup watchdog since it started.",
			float64(hb.StallWarnings), "")
	}

	// ---- managed processes ----
	head("admin_process_running", "1 when the managed process is running.", "gauge")
	infos := m.pm.List()
	for _, p := range infos {
		series("admin_process_running", svcLabel(p.Name), b2f(p.Running))
	}
	head("admin_process_failed", "1 when the process is failed (crash-loop cap or start error; see lastError).", "gauge")
	for _, p := range infos {
		series("admin_process_failed", svcLabel(p.Name), b2f(p.Status == "failed"))
	}
	head("admin_process_restarts_total", "Restarts performed for the process by the PM since admin start.", "counter")
	for _, p := range infos {
		series("admin_process_restarts_total", svcLabel(p.Name), float64(p.RestartCount))
	}
	head("admin_process_memory_bytes", "Resident memory of the managed process.", "gauge")
	for _, p := range infos {
		if p.MemoryBytes > 0 {
			series("admin_process_memory_bytes", svcLabel(p.Name), float64(p.MemoryBytes))
		}
	}

	// ---- operations ----
	gauge("admin_operation_in_progress", "1 while a long-running operation holds the progress slot.",
		b2f(m.progress.IsRunning()), "")
	pmap := m.progress.ToMap()
	lastErr := 0.0
	if e, ok := pmap["error"].(bool); ok && e {
		lastErr = 1
	}
	gauge("admin_operation_last_error", "1 when the most recent operation finished in error.",
		lastErr, "")

	if as, ok := status["autoSnapshot"].(map[string]interface{}); ok {
		gauge("admin_autosnapshot_enabled", "1 when the auto-snapshot schedule is enabled.",
			boolF(as["enabled"]), "")
	}

	// ---- http ----
	head("admin_http_requests_total", "Requests served, by method, chi route pattern and status code.", "counter")
	m.mu.Lock()
	keys := make([]string, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts := strings.SplitN(k, "|", 3)
		series("admin_http_requests_total",
			fmt.Sprintf("method=%q,route=%q,code=%q", parts[0], parts[1], parts[2]),
			float64(m.requests[k]))
	}
	m.mu.Unlock()

	// ---- runtime ----
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	gauge("admin_go_goroutines", "Live goroutines in the admin gateway.", float64(runtime.NumGoroutine()), "")
	gauge("admin_go_heap_alloc_bytes", "Heap bytes allocated and in use.", float64(ms.HeapAlloc), "")

	return b.String()
}

func nodeLabel(n map[string]interface{}) string {
	return fmt.Sprintf("node=\"%v\"", n["id"])
}

func svcLabel(name string) string {
	return fmt.Sprintf("service=%q", name)
}

func b2f(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// boolF renders an interface{} bool as 0/1 (missing/non-bool -> 0).
func boolF(v interface{}) float64 {
	b, _ := v.(bool)
	return b2f(b)
}

// boolS is 1 when the value equals the expected string.
func boolS(v interface{}, expect string) float64 {
	s, _ := v.(string)
	return b2f(s == expect)
}

// toF coerces the numeric types that appear in the status map.
func toF(v interface{}, def float64) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	default:
		return def
	}
}
