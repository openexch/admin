// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/logging"
	"github.com/openexch/admin/services"
)

type Handlers struct {
	statusSvc    *services.StatusService
	opsSvc       *services.OperationsService
	cluster      *services.Cluster
	progress     *services.Progress
	status       *services.ClusterStatus
	autoSnapshot *services.AutoSnapshot
	logSvc       *services.LogService
	procMgr      agent.ProcessAgent
	metrics      *services.MetricsService
	preflight    *services.Preflight
	cfg          *config.Config
	// clusterOps routes cluster-scoped ops (rolling-update, snapshot) to the
	// right cluster's OperationsService via the ?cluster= selector; the default
	// (empty/"match") is opsSvc, so existing callers are unchanged.
	clusterOps map[string]*services.OperationsService

	// clusterEvents is the operator-visible cluster transition log (leader
	// changes, node up/down, quorum). Registered via SetClusterEvents rather
	// than the constructor, which is already long; nil = the SSE stream simply
	// carries no cluster frames.
	clusterEvents *services.ClusterEventLog

	// journalRunner runs the per-node JournalRetention CLI. nil = the real
	// cluster exec (cluster.JournalRetention); tests override it to assert the
	// per-node fan-out without spawning java.
	journalRunner func(c *services.Cluster, nodeId int, journalRoot string, safeEgressSeq int64) (string, error)
}

func New(
	statusSvc *services.StatusService,
	opsSvc *services.OperationsService,
	clusterOps map[string]*services.OperationsService,
	cluster *services.Cluster,
	progress *services.Progress,
	status *services.ClusterStatus,
	autoSnapshot *services.AutoSnapshot,
	logSvc *services.LogService,
	procMgr agent.ProcessAgent,
	metrics *services.MetricsService,
	preflight *services.Preflight,
	cfg *config.Config,
) *Handlers {
	return &Handlers{
		statusSvc:    statusSvc,
		opsSvc:       opsSvc,
		clusterOps:   clusterOps,
		cluster:      cluster,
		progress:     progress,
		status:       status,
		autoSnapshot: autoSnapshot,
		logSvc:       logSvc,
		procMgr:      procMgr,
		metrics:      metrics,
		preflight:    preflight,
		cfg:          cfg,
	}
}

// SetClusterEvents registers the cluster transition log carried on the SSE stream.
func (h *Handlers) SetClusterEvents(l *services.ClusterEventLog) {
	h.clusterEvents = l
}

func (h *Handlers) RegisterRoutes(r chi.Router) {
	r.Use(corsMiddleware)

	// Status
	r.Get("/api/admin/status", h.handleStatus)
	r.Get("/api/admin/progress", h.handleProgress)
	r.Get("/api/admin/preflight", h.handlePreflight)
	r.Get("/api/admin/profile", h.handleGetProfile) // active runtime profile + available set
	r.Get("/api/admin/events", h.handleEvents)      // SSE: agent events + progress

	// Node operations
	r.Post("/api/admin/restart-node", h.handleRestartNode)
	r.Post("/api/admin/stop-node", h.handleStopNode)
	r.Post("/api/admin/start-node", h.handleStartNode)
	r.Post("/api/admin/stop-all-nodes", h.handleStopAllNodes)
	r.Post("/api/admin/start-all-nodes", h.handleStartAllNodes)

	// Complex operations
	r.Post("/api/admin/snapshot", h.handleSnapshot)

	// Build operations (multi-module safe)

	// Live archive reclamation: purge log segments below latest snapshot.
	// (Aeron offline ArchiveTool compaction was removed — running it against a
	// live node corrupts snapshot recordings and breaks recover-from-snapshot.)
	r.Post("/api/admin/housekeeping", h.handleHousekeeping)

	// Settlement-journal retention: purge journal-archive segments strictly below
	// a safeEgressSeq watermark, per match-cluster node. Synchronous — returns
	// per-node CLI results — because the caller wants the outcome, not a
	// fire-and-forget progress op.
	r.Post("/api/admin/journal-retention", h.handleJournalRetention)

	// Auto-snapshot (GET/POST/DELETE)
	r.Get("/api/admin/auto-snapshot", h.handleAutoSnapshotGet)
	r.Post("/api/admin/auto-snapshot", h.handleAutoSnapshotPost)
	r.Delete("/api/admin/auto-snapshot", h.handleAutoSnapshotDelete)

	// Logs
	r.Get("/api/admin/logs", h.handleLogs)

	// Self-update (admin gateway) + post-restart verification handshake

	// Durable record of the last op goroutine that died unexpectedly (ag#67):
	// survives the panic/restart that erases the in-memory Progress slot.
	r.Get("/api/admin/last-op-failure", h.handleLastOpFailure)

	// Process manager
	r.Get("/api/admin/processes", h.handleProcessList)
	r.Get("/api/admin/processes/{name}", h.handleProcessGet)
	r.Post("/api/admin/processes/{name}/start", h.handleProcessStart)
	r.Post("/api/admin/processes/{name}/stop", h.handleProcessStop)
	r.Post("/api/admin/processes/{name}/restart", h.handleProcessRestart)
	r.Post("/api/admin/processes/{name}/force-stop", h.handleProcessForceStop)
	r.Post("/api/admin/processes/start-all", h.handleProcessStartAll)
	r.Post("/api/admin/processes/stop-all", h.handleProcessStopAll)

	// Cleanup and recovery
	r.Post("/api/admin/cleanup", h.handleCleanup)
	r.Post("/api/admin/cleanup-node", h.handleCleanupNode)
	r.Get("/api/admin/backup-info", h.handleBackupInfo)
	r.Post("/api/admin/recover-from-backup", h.handleRecoverFromBackup)
	r.Post("/api/admin/reseed-node", h.handleReseedNode)

	// Health check
	r.Get("/health", h.handleHealth)

	// Prometheus metrics (auth-exempt, like /health — local scraper)
	r.Get("/metrics", h.metrics.Handler)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Private-Network", "true")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *Handlers) handleStatus(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, h.statusSvc.GetStatus())
}

func (h *Handlers) handleProgress(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("reset") == "true" && h.progress.ToMap()["complete"] == true {
		h.progress.Reset()
	}
	jsonResponse(w, http.StatusOK, h.progress.ToMap())
}

func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handlePreflight runs every invariant check on demand. Always 200: this is a
// report, never a gate — gated operations enforce blocking failures themselves.
func (h *Handlers) handlePreflight(w http.ResponseWriter, r *http.Request) {
	checks := h.preflight.RunAll()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ok":     services.InvariantsOK(checks),
		"checks": checks,
	})
}

// profileEntry renders one profile with its full field set + identity flags —
// the shared shape between GET /profile's available list and the profiles CRUD.
func (h *Handlers) profileEntry(name string, p config.Profile) map[string]interface{} {
	return map[string]interface{}{
		"name":          name,
		"builtin":       h.cfg.IsBuiltin(name),
		"description":   p.Description,
		"nodeHeapMB":    p.NodeHeapMB,
		"omsHeapMB":     p.OmsHeapMB,
		"marketHeapMB":  p.MarketHeapMB,
		"backupHeapMB":  p.BackupHeapMB,
		"preTouch":      p.PreTouch,
		"idleMode":      p.IdleMode,
		"driverProfile": p.DriverProfile,
		"driverMode":    p.DriverMode,
		"threading":     p.Threading,
		"bookCapacity":  p.BookCapacity,
		"logTermLength": p.LogTermLength,
		"minMemMB":      p.MinMemMB,
		"simGlobalOps":  p.SimGlobalOps,
		"governor":      p.Governor,
		"thp":           p.THP,
		"pinning":       p.Pinning,
	}
}

// availableProfiles renders the live set (presets + customs) in stable order.
func (h *Handlers) availableProfiles() []map[string]interface{} {
	profiles := h.cfg.ProfilesSnapshot()
	out := make([]map[string]interface{}, 0, len(profiles))
	for _, name := range config.ProfileNames(profiles) {
		out = append(out, h.profileEntry(name, profiles[name]))
	}
	return out
}

// handleGetProfile reports the active runtime profile and the full available
// set (presets + operator customs). The active fields read the LIVE profile
// (cfg.Active) so a switch is reflected immediately.
func (h *Handlers) handleGetProfile(w http.ResponseWriter, r *http.Request) {
	activeName, activeProfile := h.cfg.Active()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"active":    activeName,
		"profile":   activeProfile,
		"available": h.availableProfiles(),
	})
}

// --- Custom profiles CRUD (the presets are immutable; customs live in
// custom-profiles.json and behave exactly like presets once saved) ---

// profileNameRe keeps custom profile names file- and env-safe.
var profileNameRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// Node operations — cluster-scoped via ?cluster= (default = matching engine, so
// existing callers are unchanged). The descriptor supplies the node name + count
// and the ops service supplies the transitional-state tracker, so one code path
// drives both the matching engine and the assets engine.
func (h *Handlers) handleRestartNode(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	c, tracker := ops.Cluster(), ops.Status()
	nodeId, err := h.getNodeId(r, c.NodeCount())
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	name := c.NodeName(nodeId)
	log := logging.FromRequest(r)
	go func() {
		tracker.SetNodeStatus(nodeId, "STOPPING", false)
		if err := h.procMgr.Restart(name); err != nil {
			log.Error("restart-node failed", "cluster", c.Name, "node", nodeId, "err", err)
			tracker.SetNodeStatus(nodeId, "OFFLINE", false)
			return
		}
		tracker.SetNodeStatus(nodeId, "FOLLOWER", true)
	}()

	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Node " + strconv.Itoa(nodeId) + " restart initiated",
	})
}

func (h *Handlers) handleStopNode(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	c, tracker := ops.Cluster(), ops.Status()
	nodeId, err := h.getNodeId(r, c.NodeCount())
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	name := c.NodeName(nodeId)
	log := logging.FromRequest(r)
	go func() {
		tracker.SetNodeStatus(nodeId, "STOPPING", false)
		if err := h.procMgr.ForceStop(name); err != nil {
			log.Error("stop-node failed", "cluster", c.Name, "node", nodeId, "err", err)
		}
		tracker.SetNodeStatus(nodeId, "OFFLINE", false)
	}()

	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Node " + strconv.Itoa(nodeId) + " stop initiated",
	})
}

func (h *Handlers) handleStartNode(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	c, tracker := ops.Cluster(), ops.Status()
	nodeId, err := h.getNodeId(r, c.NodeCount())
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	name := c.NodeName(nodeId)
	log := logging.FromRequest(r)
	go func() {
		tracker.SetNodeStatus(nodeId, "STARTING", false)
		if err := h.procMgr.Start(name); err != nil {
			log.Error("start-node failed", "cluster", c.Name, "node", nodeId, "err", err)
			tracker.SetNodeStatus(nodeId, "OFFLINE", false)
			return
		}
		tracker.SetNodeStatus(nodeId, "FOLLOWER", true)
	}()

	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Node " + strconv.Itoa(nodeId) + " start initiated",
	})
}

func (h *Handlers) handleStopAllNodes(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	c, tracker := ops.Cluster(), ops.Status()
	log := logging.FromRequest(r)
	go func() {
		for i := 0; i < c.NodeCount(); i++ {
			name := c.NodeName(i)
			tracker.SetNodeStatus(i, "STOPPING", false)
			if err := h.procMgr.ForceStop(name); err != nil {
				log.Error("stop-all-nodes: node stop failed", "cluster", c.Name, "node", i, "err", err)
			}
			tracker.SetNodeStatus(i, "OFFLINE", false)
		}
	}()

	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "All nodes stop initiated",
	})
}

func (h *Handlers) handleStartAllNodes(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	c, tracker := ops.Cluster(), ops.Status()
	log := logging.FromRequest(r)
	go func() {
		for i := 0; i < c.NodeCount(); i++ {
			name := c.NodeName(i)
			tracker.SetNodeStatus(i, "STARTING", false)
			if err := h.procMgr.Start(name); err != nil {
				log.Error("start-all-nodes: node start failed", "cluster", c.Name, "node", i, "err", err)
			}
		}
		// Wait and detect leader
		leader := c.DetectLeader()
		for i := 0; i < c.NodeCount(); i++ {
			if i == leader {
				tracker.SetNodeStatus(i, "LEADER", true)
			} else {
				tracker.SetNodeStatus(i, "FOLLOWER", true)
			}
		}
	}()

	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "All nodes start initiated",
	})
}

// Complex operations
// opsFor selects the OperationsService for the ?cluster= query param (default =
// the matching engine, so existing callers are unchanged). The same code path
// serves every registered cluster.
func (h *Handlers) opsFor(r *http.Request) *services.OperationsService {
	name := r.URL.Query().Get("cluster")
	if name != "" && h.clusterOps != nil {
		if ops, ok := h.clusterOps[name]; ok {
			return ops
		}
	}
	return h.opsSvc
}

func (h *Handlers) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := h.opsFor(r).Snapshot(parseForce(r)); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Snapshot initiated",
	})
}

// parseForce reads an optional JSON body {"force": true} (match#35 lag-guard override).
func parseForce(r *http.Request) bool {
	force, _ := parseDeployRequest(r)
	return force
}

// parseDeployRequest reads {"force": bool, "ref": "..."} in one pass, because the
// body can only be decoded once.
//
// ref names which published build to deploy: a commit sha, a release tag, or
// absent for "the newest this box's channel allows". A malformed body is
// tolerated for the same reason parseForce always did — these operations are
// routinely invoked with no body at all — but note the consequence: a caller who
// misspells the ref field gets the newest build rather than an error. Named
// deploys go through the channel gate, which is where a wrong answer is caught.
func parseDeployRequest(r *http.Request) (bool, string) {
	var body struct {
		Force bool   `json:"force"`
		Ref   string `json:"ref"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	return body.Force, body.Ref
}

func (h *Handlers) handleHousekeeping(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	// Capability refusal BEFORE the shared operation slot is claimed: a cluster with
	// no housekeeping tool must not wedge the global Progress (the #26 lesson).
	if ops.Cluster().HousekeepingMain == "" {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "cluster '" + ops.Cluster().Name + "' has no archive housekeeping",
		})
		return
	}
	if err := ops.Housekeeping(parseForce(r)); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Archive housekeeping initiated",
	})
}

// handleJournalRetention purges settlement-journal archive segments strictly
// below a caller-supplied safeEgressSeq watermark, per match-cluster node. It
// execs the match-cluster JournalRetention CLI on each node (live-safe, mirroring
// the housekeeping fan-out) and returns the per-node CLI output as JSON.
//
// The watermark is the egressSeq up to which settlement has been durably applied
// downstream (the Assets Engine); nothing at or above it is reclaimed. There is
// NO automatic scheduling here on purpose — the auto-hook that feeds this the
// AE-snapshot watermark arrives with the settlement bridge; until then this
// endpoint is operator-driven.
func (h *Handlers) handleJournalRetention(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SafeEgressSeq int64 `json:"safeEgressSeq"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	// Nothing is provably safe to purge below a non-positive watermark; refuse
	// before touching any node (the CLI enforces this too, defence in depth).
	if req.SafeEgressSeq <= 0 {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "safeEgressSeq must be > 0",
		})
		return
	}
	journalRoot := h.cfg.SettlementJournalDir
	if journalRoot == "" {
		jsonResponse(w, http.StatusConflict, map[string]string{
			"error": "journal not configured (SETTLEMENT_JOURNAL_DIR unset — feature off)",
		})
		return
	}

	c := h.cluster // the settlement journal is a match-cluster feature
	run := h.journalRunner
	if run == nil {
		run = func(cl *services.Cluster, nodeId int, root string, seq int64) (string, error) {
			return cl.JournalRetention(nodeId, root, seq)
		}
	}

	log := logging.FromRequest(r)
	results := make([]map[string]interface{}, 0, c.NodeCount())
	failures := 0
	for i := 0; i < c.NodeCount(); i++ {
		output, err := run(c, i, journalRoot, req.SafeEgressSeq)
		entry := map[string]interface{}{"node": i, "output": output}
		if err != nil {
			failures++
			entry["error"] = err.Error()
			log.Warn("journal-retention failed on node", "cluster", c.Name, "node", i, "err", err)
		}
		log.Info("journal-retention node output", "cluster", c.Name, "node", i, "output", output)
		results = append(results, entry)
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"safeEgressSeq": req.SafeEgressSeq,
		"results":       results,
		"failures":      failures,
	})
}

// Auto-snapshot handlers
func (h *Handlers) handleAutoSnapshotGet(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, h.autoSnapshot.ToMap())
}

func (h *Handlers) handleAutoSnapshotPost(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IntervalMinutes int64 `json:"intervalMinutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IntervalMinutes <= 0 {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "intervalMinutes must be a positive number",
		})
		return
	}

	h.autoSnapshot.Start(req.IntervalMinutes)
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"status":          "started",
		"intervalMinutes": req.IntervalMinutes,
		"message":         "Auto-snapshot enabled: every " + strconv.FormatInt(req.IntervalMinutes, 10) + " minutes",
	})
}

func (h *Handlers) handleAutoSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	h.autoSnapshot.Stop()
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"status":  "stopped",
		"message": "Auto-snapshot disabled",
	})
}

// Logs handler
func (h *Handlers) handleLogs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	lines := 50
	if l := query.Get("lines"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			lines = parsed
			if lines > 500 {
				lines = 500
			}
		}
	}

	if service := query.Get("service"); service != "" {
		jsonResponse(w, http.StatusOK, h.logSvc.GetServiceLogs(service, lines))
		return
	}

	nodeId := 0
	if n := query.Get("node"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil {
			nodeId = parsed
		}
	}

	// Cluster-aware node log file: node<id> for the matching engine (default),
	// ae<id> for the assets engine, resolved from the ?cluster= descriptor.
	name := h.opsFor(r).Cluster().NodeName(nodeId)
	jsonResponse(w, http.StatusOK, h.logSvc.GetNodeLogsNamed(name, nodeId, lines))
}

// Cleanup handler
func (h *Handlers) handleCleanup(w http.ResponseWriter, r *http.Request) {
	var opts services.CleanupOptions
	json.NewDecoder(r.Body).Decode(&opts) // ignore error - defaults to false values
	result := h.opsFor(r).Cleanup(opts)
	status := http.StatusOK
	if success, ok := result["success"].(bool); ok && !success {
		status = http.StatusBadRequest
	}
	jsonResponse(w, status, result)
}

// CleanupNode handler for per-node cleanup
func (h *Handlers) handleCleanupNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeId int  `json:"nodeId"`
		Force  bool `json:"force"`
		DryRun bool `json:"dryRun"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	result := h.opsFor(r).CleanupNode(req.NodeId, req.Force, req.DryRun)
	status := http.StatusOK
	if success, ok := result["success"].(bool); ok && !success {
		status = http.StatusBadRequest
	}
	jsonResponse(w, status, result)
}

// BackupInfo handler
func (h *Handlers) handleBackupInfo(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	if ops.Cluster().BackupDir == "" {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "cluster '" + ops.Cluster().Name + "' has no backup",
		})
		return
	}
	jsonResponse(w, http.StatusOK, ops.GetBackupInfo())
}

// RecoverFromBackup handler
func (h *Handlers) handleRecoverFromBackup(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	if ops.Cluster().BackupDir == "" {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "cluster '" + ops.Cluster().Name + "' has no backup to recover from",
		})
		return
	}
	var req struct {
		NodeId int  `json:"nodeId"`
		Force  bool `json:"force"`
		DryRun bool `json:"dryRun"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	result := ops.RecoverFromBackup(req.NodeId, req.Force, req.DryRun)
	status := http.StatusOK
	if success, ok := result["success"].(bool); ok && !success {
		status = http.StatusBadRequest
	}
	jsonResponse(w, status, result)
}

// --- Process Manager Handlers ---

func (h *Handlers) handleProcessList(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, h.procMgr.List())
}

func (h *Handlers) handleProcessGet(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	info := h.procMgr.Get(name)
	if info == nil {
		jsonResponse(w, http.StatusNotFound, map[string]string{"error": "unknown service: " + name})
		return
	}
	jsonResponse(w, http.StatusOK, info)
}

func (h *Handlers) handleProcessStart(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.procMgr.Start(name); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": name + " start initiated",
	})
}

func (h *Handlers) handleProcessStop(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.procMgr.Stop(name); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": name + " stop initiated",
	})
}

func (h *Handlers) handleProcessRestart(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.procMgr.Restart(name); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": name + " restart initiated",
	})
}

func (h *Handlers) handleProcessForceStop(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.procMgr.ForceStop(name); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": name + " force-stop initiated",
	})
}

func (h *Handlers) handleProcessStartAll(w http.ResponseWriter, r *http.Request) {
	go func() {
		// Runs in background — dependency-ordered start takes time
		h.procMgr.StartAll()
	}()
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Start-all initiated (dependency-ordered)",
	})
}

func (h *Handlers) handleProcessStopAll(w http.ResponseWriter, r *http.Request) {
	go func() {
		h.procMgr.StopAll()
	}()
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": "Stop-all initiated (reverse dependency order)",
	})
}

// handleReseedNode launches the stranded-member reseed: copy a healthy
// follower's state over a corrupt member's (match#35 procedure, automated).
func (h *Handlers) handleReseedNode(w http.ResponseWriter, r *http.Request) {
	ops := h.opsFor(r)
	// Reseed copies a healthy follower's state over a stranded member: it needs a
	// distinct source, so a single-node cluster has nothing to reseed from. Refuse
	// before the shared operation slot is claimed.
	if ops.Cluster().NodeCount() < 2 {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "cluster '" + ops.Cluster().Name + "' is single-node; reseed needs a source follower",
		})
		return
	}
	var req struct {
		NodeId       *int `json:"nodeId"`
		SourceNodeId *int `json:"sourceNodeId"`
		Force        bool `json:"force"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeId == nil || req.SourceNodeId == nil {
		jsonResponse(w, http.StatusBadRequest, map[string]string{
			"error": "body must be {\"nodeId\": <stranded>, \"sourceNodeId\": <healthy follower>, \"force\": true}",
		})
		return
	}
	if err := ops.ReseedNode(*req.NodeId, *req.SourceNodeId, req.Force); err != nil {
		jsonResponse(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusAccepted, map[string]string{
		"message": fmt.Sprintf("Reseeding node%d from node%d — the source follower stops during the copy "+
			"(brief quorum outage). Poll /api/admin/progress.", *req.NodeId, *req.SourceNodeId),
	})
}

// handleLastOpFailure reports the durable record of the last operation goroutine
// that died unexpectedly (a panic, or an admin restart mid-op) — the failure the
// in-memory Progress slot loses when its goroutine dies (ag#67). state="none"
// when no op has died since the record was last cleared.
func (h *Handlers) handleLastOpFailure(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, h.opsSvc.LastOpFailure())
}

// Helpers
func (h *Handlers) getNodeId(r *http.Request, nodeCount int) (int, error) {
	var req struct {
		NodeId int `json:"nodeId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return 0, err
	}
	if req.NodeId < 0 || req.NodeId >= nodeCount {
		return 0, &InvalidNodeError{max: nodeCount - 1}
	}
	return req.NodeId, nil
}

type InvalidNodeError struct{ max int }

func (e *InvalidNodeError) Error() string {
	return fmt.Sprintf("Invalid nodeId. Must be 0..%d", e.max)
}

func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
