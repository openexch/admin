// SPDX-License-Identifier: Apache-2.0
package services

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/logging"
)

// OperationsService handles complex cluster operations
type OperationsService struct {
	cfg           *config.Config
	cluster       *Cluster
	peers         []*Cluster // OTHER registered clusters; their dirs are off-limits to this cluster's cleanup
	progress      *Progress
	clusterStatus *ClusterStatus
	procMgr       agent.ProcessAgent
	statusSvc     *StatusService
	// ledgerArchive is the off-box half. Defaults to NoLedgerArchive, which
	// reports nothing durable and constrains nothing.
	ledgerArchive LedgerArchiver
	// watermarks computes how far the log is reclaimable; nil = the old
	// snapshot-only purge.
	watermarks *WatermarkTracker
	preflight  *Preflight
	// bundler, when set, turns a fresh snapshot into a durable off-box copy.
	// Registered by the operations layer above this core; nil means the box
	// keeps its snapshots locally, which is still a restore point.
	bundler func(log *slog.Logger, snapshotPosition int64)
	log     *slog.Logger
}

func NewOperationsService(cfg *config.Config, cluster *Cluster, progress *Progress, status *ClusterStatus) *OperationsService {
	o := &OperationsService{
		cfg:           cfg,
		cluster:       cluster,
		progress:      progress,
		clusterStatus: status,
		ledgerArchive: NoLedgerArchive,
		log:           logging.Component("ops"),
	}
	return o
}

// SetPeerClusters registers the OTHER clusters on this box, whose state/driver
// dirs this service's cleanup must never touch.
func (o *OperationsService) SetPeerClusters(peers []*Cluster) { o.peers = peers }

// Cluster returns the descriptor this ops service manages. Cluster-scoped handlers
// resolve the ops via ?cluster= (opsFor) and read the descriptor from here, so one
// code path serves every cluster (node names, count, capability flags, DetectLeader).
func (o *OperationsService) Cluster() *Cluster { return o.cluster }

// Status returns this cluster's transitional-state tracker (STOPPING/STARTING/…),
// written by the node lifecycle handlers and read by the status poller.
func (o *OperationsService) Status() *ClusterStatus { return o.clusterStatus }

// SetProcessManager injects the process agent (avoids circular init)
func (o *OperationsService) SetProcessManager(pm agent.ProcessAgent) {
	o.procMgr = pm
}

// SetStatusService injects the status service for the archive-op lag guard.
func (o *OperationsService) SetStatusService(s *StatusService) {
	o.statusSvc = s
}

// SetLedgerArchive injects the off-box archive whose verified position may
// hold the retention watermark back. A nil argument keeps the no-op default.
func (o *OperationsService) SetLedgerArchive(a LedgerArchiver) {
	if a == nil {
		a = NoLedgerArchive
	}
	o.ledgerArchive = a
}

// archiver is the read side. A zero-value OperationsService (tests build them
// as literals) leaves the interface nil, and the retention path must not be the
// place that panics over it.
func (o *OperationsService) archiver() LedgerArchiver {
	if o.ledgerArchive == nil {
		return NoLedgerArchive
	}
	return o.ledgerArchive
}

// SetSnapshotBundler registers the hook that makes a snapshot durable off the
// box. It runs BEFORE reclamation, because a bundle needs the log segments the
// purge is about to delete, and a failure inside it must never block the purge:
// making reclamation depend on a remote bucket is how the /dev/shm outage comes
// back from a new cause.
func (o *OperationsService) SetSnapshotBundler(f func(log *slog.Logger, snapshotPosition int64)) {
	o.bundler = f
}

// SetWatermarkTracker injects the retention-watermark calculator.
func (o *OperationsService) SetWatermarkTracker(w *WatermarkTracker) {
	o.watermarks = w
}

// SetPreflight injects the invariant engine that gates destructive operations.
func (o *OperationsService) SetPreflight(p *Preflight) {
	o.preflight = p
}

// gate runs the pre-flight gate for op inside an already-claimed progress
// slot, releasing the slot on refusal (an early return without Finish would
// wedge every future operation — the #26 lesson, same shape as Snapshot's
// lag guard).
func (o *OperationsService) gate(op string, force bool) error {
	if o.preflight == nil {
		return nil
	}
	// Cluster-scoped: the match-specific checks (quorum, driver-dirs) are skipped
	// for other clusters (e.g. the single-node assets engine) while the global
	// mem/disk gates still apply — see Preflight.GateForCluster (ag#83). A nil
	// cluster (some unit tests) defaults to the matching-engine semantics.
	clusterName := matchClusterName
	if o.cluster != nil {
		clusterName = o.cluster.Name
	}
	if err := o.preflight.GateForCluster(op, clusterName, force); err != nil {
		o.progress.Finish(false, "refused: "+err.Error())
		return err
	}
	return nil
}

// archiveOpBlockReason returns a non-empty reason when PURGING would risk
// stranding a member (match#35). Housekeeping deletes log segments below the
// latest snapshot, and a member that was not caught up past that point can then
// never rejoin: it asks the leader to truncate to a position that no longer
// exists and loops forever. Confirmed in production 2026-07-25:
//
//	ArchiveException: invalid position 0: start=872415232 stop=872415232
//	  at ConsensusModuleAgent.truncateLogEntry <- Election.onNewLeadershipTerm
//
// So Aeron does NOT catch a wiped member up from the leader's snapshot; only
// a reseed recovers it. The guard is real and stays.
//
// It guards the PURGE alone. Taking a snapshot is additive — it only writes new
// recordings — and is exactly what a recovering member needs, so gating the
// whole operation on health turned one unhealthy member into zero snapshots on
// both clusters for sixteen hours, an unbounded log, and a full /dev/shm.
//
// Scoped to THIS cluster. It previously read the top-level allNodesHealthy /
// nodes fields, which are always the matching engine's, so an assets-cluster op
// would have been judged by the ME's health.
func (o *OperationsService) archiveOpBlockReason() string {
	if o.statusSvc == nil {
		return ""
	}
	healthy, nodes := o.clusterHealthFromStatus(o.statusSvc.GetStatus())
	if healthy {
		return ""
	}
	detail := ""
	for _, n := range nodes {
		if h, _ := n["health"].(string); h != "" && h != "HEALTHY" {
			name, _ := n["procName"].(string)
			if name == "" {
				name = fmt.Sprintf("node%v", n["id"])
			}
			detail += " " + name + "=" + h
		}
	}
	if detail == "" {
		detail = " (health fields unavailable)"
	}
	return "cluster not fully healthy:" + detail
}

// clusterHealthFromStatus finds this cluster's own health block in the status
// payload, falling back to the top-level fields (the matching engine's) only
// when the generic clusters array is absent.
func (o *OperationsService) clusterHealthFromStatus(s map[string]interface{}) (bool, []map[string]interface{}) {
	if raw, ok := s["clusters"].([]map[string]interface{}); ok {
		for _, c := range raw {
			if name, _ := c["name"].(string); name == o.cluster.Name {
				healthy, _ := c["allNodesHealthy"].(bool)
				nodes, _ := c["nodes"].([]map[string]interface{})
				return healthy, nodes
			}
		}
	}
	healthy, _ := s["allNodesHealthy"].(bool)
	nodes, _ := s["nodes"].([]map[string]interface{})
	return healthy, nodes
}

// LogBytesSinceSnapshot is how far this cluster's log has run past its newest
// snapshot, in bytes — the largest value across members, so the trigger follows
// whichever node has the most to lose.
//
// This is the same logDelta the console shows. Aeron log positions are byte
// offsets, so the subtraction is already the answer; nothing needs measuring.
// Returns -1 when the status cannot answer, which callers must treat as "do not
// trigger" rather than "zero".
func (o *OperationsService) LogBytesSinceSnapshot() int64 {
	if o.statusSvc == nil {
		return -1
	}
	_, nodes := o.clusterHealthFromStatus(o.statusSvc.GetStatus())
	var max int64 = -1
	for _, n := range nodes {
		delta, ok := n["logDelta"].(int64)
		if !ok {
			continue
		}
		if delta > max {
			max = delta
		}
	}
	return max
}

// isNodeRunning checks if a node is running via ProcessManager
func (o *OperationsService) isNodeRunning(nodeId int) bool {
	if o.procMgr == nil {
		return false // No PM means can't determine state
	}
	info := o.procMgr.Get(o.cluster.NodeName(nodeId))
	return info != nil && info.Running
}

// startService starts a service via process manager
func (o *OperationsService) startService(name string) {
	if o.procMgr == nil {
		o.log.Error("process manager not initialized, cannot start service", "service", name)
		return
	}
	if err := o.procMgr.StartUnchecked(name); err != nil {
		o.log.Error("start service failed", "service", name, "err", err)
	}
}

// stopService stops a service via process manager
func (o *OperationsService) stopService(name string) {
	if o.procMgr == nil {
		o.log.Error("process manager not initialized, cannot stop service", "service", name)
		return
	}
	if err := o.procMgr.ForceStop(name); err != nil {
		o.log.Error("stop service failed", "service", name, "err", err)
	}
}

// restartService restarts a service via process manager
func (o *OperationsService) restartService(name string) {
	if o.procMgr == nil {
		o.log.Error("process manager not initialized, cannot restart service", "service", name)
		return
	}
	if err := o.procMgr.Restart(name); err != nil {
		o.log.Error("restart service failed", "service", name, "err", err)
	}
}

// Snapshot triggers a cluster snapshot. It is NOT gated on cluster health: a
// snapshot only adds recordings, and refusing it while a member is unhealthy is
// what produced the 2026-07-25 outage — sixteen hours with zero snapshots on
// either cluster, an unbounded cluster log, and a full /dev/shm. The health
// guard applies to the reclamation that follows (see doSnapshot), because
// purging is the part that can strand a member.
func (o *OperationsService) Snapshot(force bool) error {
	if !o.progress.TryStart("snapshot", 7) {
		return fmt.Errorf("another operation in progress")
	}
	go o.doSnapshot(force)
	return nil
}

func (o *OperationsService) doSnapshot(force bool) {
	defer o.recoverOp() // ag#67: contain+record a panic, free the slot
	log := o.log.With("op", "snapshot", "op_id", o.progress.CurrentOpID())
	// Step 1: Find leader
	o.progress.Update(1, "Finding cluster leader...")
	leader := o.cluster.DetectLeader()
	if leader < 0 {
		o.progress.Finish(false, "Could not find cluster leader")
		return
	}

	// Step 2: Take snapshot
	o.progress.Update(2, fmt.Sprintf("Taking snapshot on Node %d...", leader))
	output, err := o.cluster.TakeSnapshot(leader)
	if err != nil {
		o.progress.Finish(false, "Snapshot failed: "+err.Error())
		return
	}

	// Step 3: Wait for propagation
	o.progress.Update(3, "Waiting for snapshot propagation...")
	time.Sleep(2 * time.Second)

	// Step 4: Verify
	o.progress.Update(4, "Verifying snapshot position...")
	pos := o.cluster.GetSnapshotPosition(leader)

	if pos < 0 || (!contains(output, "SNAPSHOT") && !contains(output, "completed")) {
		o.progress.Finish(false, "Snapshot may have failed: "+output)
		return
	}

	// Capture BEFORE the reclaim below. The bundle needs the log segments the
	// purge is about to delete, so the order is a correctness requirement, not a
	// preference.
	//
	// A capture or upload failure does NOT block the purge, deliberately. Making
	// reclamation depend on S3 without a bound is how you rebuild the outage from
	// a new cause: bucket unreachable, no purge, /dev/shm fills, the money path
	// stops again. Step 02 introduces that dependency together with the escape
	// valve it needs. Until then this strictly improves on the old behaviour —
	// sometimes a bundle exists where previously none ever did — and a failure is
	// loud in the log and in /status rather than silent.
	// The seam: the core produces the snapshot, an off-box archiver turns it
	// into something durable elsewhere. Nil here is the normal single-box case,
	// not a degraded one — bundles with nowhere to ship only fill local disk
	// with copies of what is already local.
	if o.bundler != nil {
		o.bundler(log, pos)
	}

	// Steps 5-7: Reclaim archive disk on each node. The snapshot makes the log
	// below its position unnecessary, but Aeron never reclaims automatically —
	// purge log segments while the cluster runs.
	//
	// THIS is where the match#35 guard belongs: purging strands any member that
	// is not caught up past the snapshot. The snapshot above already succeeded
	// and is durable, so a blocked reclaim costs disk, never a restore point.
	if !force {
		if reason := o.archiveOpBlockReason(); reason != "" {
			log.Warn("archive reclamation skipped", "reason", reason)
			o.progress.Finish(true, fmt.Sprintf(
				"Snapshot created at position %d. Archive reclamation SKIPPED: %s — "+
					"the log will keep growing until every member is healthy "+
					"(match#35: purging strands a member that cannot catch up)", pos, reason))
			return
		}
	}

	// How far the log is reclaimable: the lowest position every consumer has
	// already passed. Computed and reported unconditionally; applied only when
	// WATERMARK_RETENTION is on (see the config field for why those are split).
	watermark := o.retentionWatermark(log, pos)

	housekeepingFailures := 0
	for i := 0; i < o.cluster.NodeCount(); i++ {
		o.progress.Update(5+i, fmt.Sprintf("Reclaiming archive on Node %d...", i))
		hkOutput, hkErr := o.cluster.ArchiveHousekeepingTo(i, watermark)
		log.Info("node housekeeping output", "node", i, "output", hkOutput)
		if hkErr != nil {
			housekeepingFailures++
			log.Warn("housekeeping failed on node", "node", i, "err", hkErr)
		}
	}

	if housekeepingFailures > 0 {
		o.progress.Finish(true, fmt.Sprintf(
			"Snapshot created at position %d, but archive reclamation failed on %d node(s) — check logs",
			pos, housekeepingFailures))
		return
	}
	o.progress.Finish(true, fmt.Sprintf("Snapshot created at position %d, archives reclaimed", pos))
}

// Housekeeping reclaims archive disk on all nodes without taking a snapshot
// (uses the latest existing snapshot as the purge boundary).
func (o *OperationsService) Housekeeping(force bool) error {
	if !o.progress.TryStart("housekeeping", 3) {
		return fmt.Errorf("another operation in progress")
	}
	if !force {
		if reason := o.archiveOpBlockReason(); reason != "" {
			o.progress.Finish(false, "refused: "+reason)
			return fmt.Errorf("refusing housekeeping: %s (match#35 lag guard; POST {\"force\":true} to override)", reason)
		}
	}

	go o.doHousekeeping()
	return nil
}

func (o *OperationsService) doHousekeeping() {
	defer o.recoverOp() // ag#67: contain+record a panic, free the slot
	log := o.log.With("op", "housekeeping", "op_id", o.progress.CurrentOpID())
	failures := 0
	for i := 0; i < o.cluster.NodeCount(); i++ {
		o.progress.Update(1+i, fmt.Sprintf("Reclaiming archive on Node %d...", i))
		output, err := o.cluster.ArchiveHousekeeping(i)
		log.Info("node housekeeping output", "node", i, "output", output)
		if err != nil {
			failures++
			log.Warn("housekeeping failed on node", "node", i, "err", err)
		}
	}

	if failures > 0 {
		o.progress.Finish(false, fmt.Sprintf("Archive reclamation failed on %d node(s) — check logs", failures))
		return
	}
	o.progress.Finish(true, "Archives reclaimed on all nodes")
}

// CleanupOptions configures the cleanup operation
type CleanupOptions struct {
	Force  bool `json:"force"`
	DryRun bool `json:"dryRun"`
	Backup bool `json:"backup"`
	// Archives are PRESERVED by default (#10). Wiping them additionally
	// requires ConfirmArchiveLoss to spell out the exact phrase below.
	IncludeArchive     bool   `json:"includeArchive"`
	ConfirmArchiveLoss string `json:"confirmArchiveLoss"`
}

// The second confirmation required to wipe cluster archives via /cleanup.
const archiveLossConfirmation = "DELETE-CLUSTER-STATE"

// cleanupSweep removes Aeron IPC dirs (under shmDir/tmpDir) and this cluster's
// mark files, locks, and archives (under stateRoot) FOR ONE CLUSTER. stateRoot
// is that cluster's ACTUAL state+archive root (o.cluster.StateDir): its archives
// are PRESERVED unless includeArchive — /cleanup used to run `rm -rf
// /dev/shm/aeron-*`, and that glob matches aeron-cluster — nuking the very
// archives P1.3 makes durable (#10).
//
// stateRoot is used DIRECTLY (not shmDir/<basename>): the ME state root moved
// off /dev/shm to disk on 2026-07-13 (env MATCH_STATE_DIR), so deriving the
// archive path from shmDir silently missed it — the ME restored a stale snapshot
// while the AE reset to genesis, and the tradeId mismatch HALTED the settlement
// bridge (no money settles, all holds leak). The /dev/shm IPC/driver sweep
// (steps 1,4) still uses shmDir, excluding THIS cluster's dir by basename.
//
// exclude lists OTHER clusters' dir base names (state dirs + driver dirs): a
// match cleanup must never touch /dev/shm/aeron-assets and vice versa (the
// 2026-07-09 clean-slate wiped the assets engine's state under a live ae0 before
// this scoping existed). driverGuard vets each aeron-* entry that IS one of THIS
// cluster's node media-driver dirs through canDeleteDriverDir (the #42/ag#68
// guard) so the sweep can never delete a driver dir out from under a live driver
// — it was the only unguarded /dev/shm driver-dir deleter before ag#68 (nil =
// vet nothing, used by the pure archive/scoping unit tests). Refused entries are
// returned in refused, not deleted. apply=false only reports. Factored out with
// configurable roots so every guarantee is unit-testable.
func cleanupSweep(shmDir, tmpDir, stateRoot string, exclude map[string]bool, driverGuard func(base, path string) (ok bool, reason string), includeArchive, apply bool) (cleaned, preserved, errs, refused []string) {
	// Basename is used only to exclude THIS cluster's dir from the /dev/shm IPC
	// glob (step 1); the archive/mark wipe (steps 2-3) targets stateRoot directly.
	stateBase := filepath.Base(stateRoot)
	remove := func(path string, all bool) {
		cleaned = append(cleaned, path)
		if !apply {
			return
		}
		var err error
		if all {
			err = os.RemoveAll(path)
		} else {
			err = os.Remove(path)
		}
		if err != nil && !os.IsNotExist(err) {
			errs = append(errs, path+": "+err.Error())
		}
	}

	// 1. Aeron IPC dirs (drivers, clients) — everything aeron-* EXCEPT this
	// cluster's state dir (the old glob wrongly swallowed it) and every other
	// cluster's dirs (state + drivers), which are not ours to touch. A node
	// media-driver dir is deleted only if driverGuard clears it (ag#68).
	entries, _ := filepath.Glob(filepath.Join(shmDir, "aeron-*"))
	for _, e := range entries {
		base := filepath.Base(e)
		if base == stateBase || exclude[base] {
			continue
		}
		if driverGuard != nil {
			if ok, reason := driverGuard(base, e); !ok {
				refused = append(refused, e+": "+reason)
				continue
			}
		}
		remove(e, true)
	}

	// 2. Stale mark/lock files inside the cluster state dirs (node*, backup),
	// under the REAL state root (disk or /dev/shm), not shmDir/<basename>.
	for _, pattern := range []string{
		"*/cluster/cluster-mark*.dat",
		"*/cluster/*.lck",
		"*/archive/archive-mark.dat",
	} {
		matches, _ := filepath.Glob(filepath.Join(stateRoot, pattern))
		for _, m := range matches {
			remove(m, false)
		}
	}

	// 3. The archives themselves: preserved unless explicitly included. Target
	// stateRoot directly so a disk-based ME state (MATCH_STATE_DIR) is actually
	// wiped — the bug that left a stale ME snapshot and halted the bridge.
	recordings, _ := filepath.Glob(filepath.Join(stateRoot, "*/archive/*.rec"))
	clusterDir := stateRoot
	if includeArchive {
		if _, err := os.Stat(clusterDir); err == nil {
			cleaned = append(cleaned, fmt.Sprintf("%s (FULL WIPE incl. %d recording(s))", clusterDir, len(recordings)))
			if apply {
				if err := os.RemoveAll(clusterDir); err != nil && !os.IsNotExist(err) {
					errs = append(errs, clusterDir+": "+err.Error())
				}
			}
		}
	} else if len(recordings) > 0 {
		preserved = append(preserved, fmt.Sprintf(
			"%d archive recording(s) under %s preserved — pass includeArchive=true with confirmArchiveLoss=%q to wipe",
			len(recordings), clusterDir, archiveLossConfirmation))
	}

	// 4. Gateway/client Aeron dirs under tmp (same exclusions apply)
	tmpEntries, _ := filepath.Glob(filepath.Join(tmpDir, "aeron-*"))
	for _, e := range tmpEntries {
		if exclude[filepath.Base(e)] {
			continue
		}
		remove(e, true)
	}

	return cleaned, preserved, errs, refused
}

// driverDirGuard returns the per-entry deletion guard cleanupSweep applies to
// aeron-* entries that are THIS cluster's node media-driver dirs (ag#68). It
// maps the driver-dir base name back to the owning node/driver services and
// routes the delete decision through canDeleteDriverDir with their live state,
// so the sweep can never delete a dir out from under a live driver — including
// the embedded-driver case (assets always, matching engine on the light
// profile) where there is no external driver pid file to trust. Entries that
// are NOT this cluster's driver dirs (client/gateway IPC dirs, etc.) always
// clear (ok=true), preserving the existing stale-IPC reclaim.
func (o *OperationsService) driverDirGuard() func(base, path string) (bool, string) {
	type owner struct{ node, driver string }
	owners := map[string]owner{}
	for i := 0; i < o.cluster.NodeCount(); i++ {
		b := filepath.Base(o.cluster.DriverAeronDir(i))
		owners[b] = owner{node: o.cluster.NodeName(i), driver: o.cluster.DriverName(i)}
	}
	return func(base, path string) (bool, string) {
		ow, ok := owners[base]
		if !ok {
			return true, "" // not one of this cluster's driver dirs
		}
		driverTracked, nodeRunning := false, false
		if o.procMgr != nil {
			if ow.driver != "" {
				if info := o.procMgr.Get(ow.driver); info != nil && info.Running {
					driverTracked = true
				}
			}
			if info := o.procMgr.Get(ow.node); info != nil && info.Running {
				nodeRunning = true
			}
		}
		return canDeleteDriverDir(path, driverTracked, nodeRunning)
	}
}

// peerClusterDirs is the exclusion set for this cluster's sweep: every OTHER
// registered cluster's state-dir and per-node driver-dir base names.
func (o *OperationsService) peerClusterDirs() map[string]bool {
	exclude := map[string]bool{}
	for _, p := range o.peers {
		exclude[filepath.Base(p.StateDir)] = true
		for i := 0; i < p.NodeCount(); i++ {
			exclude[filepath.Base(p.DriverAeronDir(i))] = true
		}
	}
	return exclude
}

// Cleanup removes stale Aeron files (requires all nodes stopped and force=true)
func (o *OperationsService) Cleanup(opts CleanupOptions) map[string]interface{} {
	result := map[string]interface{}{}

	// Dry-run changes nothing: allow it anytime (even with nodes running) so
	// ops can preview the sweep and the archive-preservation notice.
	if opts.DryRun {
		wouldClean, preserved, _, refused := cleanupSweep("/dev/shm", "/tmp",
			o.cluster.StateDir, o.peerClusterDirs(), o.driverDirGuard(), opts.IncludeArchive, false)
		result["success"] = true
		result["dryRun"] = true
		result["wouldClean"] = wouldClean
		if len(preserved) > 0 {
			result["preserved"] = preserved
		}
		if len(refused) > 0 {
			result["refused"] = refused
		}
		return result
	}

	// Require force flag for destructive operation
	if !opts.Force {
		result["success"] = false
		result["error"] = "Destructive operation requires force=true"
		return result
	}

	// Check if any nodes are running via ProcessManager
	for i := 0; i < o.cluster.NodeCount(); i++ {
		if o.isNodeRunning(i) {
			result["success"] = false
			result["error"] = fmt.Sprintf("Node %d is still running. Stop all nodes before cleanup.", i)
			return result
		}
	}

	// External media drivers own /dev/shm/aeron-<user>-N-driver, which the wipe below
	// deletes — they must be stopped too or their IPC files are pulled out from under them.
	// An embedded-driver cluster has no separate driver services to check.
	if o.procMgr != nil && !o.cluster.Embedded {
		for i := 0; i < o.cluster.NodeCount(); i++ {
			if info := o.procMgr.Get(o.cluster.DriverName(i)); info != nil && info.Running {
				result["success"] = false
				result["error"] = fmt.Sprintf("Media driver %d is still running. Stop all drivers before cleanup.", i)
				return result
			}
		}
	}

	// Wiping archives destroys the recovery source (#10): demand an explicit
	// second confirmation beyond force=true.
	if opts.IncludeArchive && opts.ConfirmArchiveLoss != archiveLossConfirmation {
		result["success"] = false
		result["error"] = fmt.Sprintf(
			"includeArchive=true wipes ALL cluster archives; set confirmArchiveLoss=%q to confirm",
			archiveLossConfirmation)
		return result
	}

	// Backup mark files before cleanup if requested
	if opts.Backup {
		backupPath := o.backupMarkFiles()
		result["backupCreated"] = backupPath
	}

	cleaned, preserved, errors, refused := cleanupSweep("/dev/shm", "/tmp",
		o.cluster.StateDir, o.peerClusterDirs(), o.driverDirGuard(), opts.IncludeArchive, true)

	// Log every deletion and every refusal to the admin log (component=ops), not
	// just the API response, so a destructive sweep leaves an audit trail even if
	// the caller never reads the body (ag#68).
	for _, path := range cleaned {
		o.log.Info("cleanup removed", "cluster", o.cluster.Name, "path", path)
	}
	for _, r := range refused {
		o.log.Warn("cleanup refused driver dir (ag#68 guard)", "cluster", o.cluster.Name, "detail", r)
	}

	result["success"] = len(errors) == 0
	result["cleaned"] = cleaned
	if len(preserved) > 0 {
		result["preserved"] = preserved
	}
	if len(refused) > 0 {
		result["refused"] = refused
	}
	if len(errors) > 0 {
		result["errors"] = errors
		result["message"] = "Cleanup completed with some errors."
	} else {
		result["message"] = "Cleanup completed successfully. You can now start the cluster."
	}

	return result
}

// CleanupNode removes stale Aeron files for a single node
func (o *OperationsService) CleanupNode(nodeId int, force, dryRun bool) map[string]interface{} {
	result := map[string]interface{}{"nodeId": nodeId}

	if nodeId < 0 || nodeId >= o.cluster.NodeCount() {
		result["success"] = false
		result["error"] = fmt.Sprintf("Invalid nodeId (must be 0..%d)", o.cluster.NodeCount()-1)
		return result
	}

	if !force {
		result["success"] = false
		result["error"] = "Destructive operation requires force=true"
		return result
	}

	if o.isNodeRunning(nodeId) {
		result["success"] = false
		result["error"] = fmt.Sprintf("Node %d is still running", nodeId)
		return result
	}

	nodeDir := o.cluster.NodeStateDir(nodeId)
	files := []string{
		nodeDir + "/cluster/cluster-mark*.dat",
		nodeDir + "/cluster/*.lck",
		nodeDir + "/archive/archive-mark.dat",
	}

	if dryRun {
		result["success"] = true
		result["dryRun"] = true
		result["wouldClean"] = files
		return result
	}

	// Clean files — glob in-process, no shell (admin-gateway#11)
	for _, pattern := range files {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, match := range matches {
			os.Remove(match)
		}
	}

	result["success"] = true
	result["cleaned"] = files
	result["message"] = fmt.Sprintf("Node %d mark files cleaned", nodeId)
	return result
}

// BackupInfo contains information about available backups
type BackupInfo struct {
	BackupDir       string `json:"backupDir"`
	HasRecordingLog bool   `json:"hasRecordingLog"`
	HasArchive      bool   `json:"hasArchive"`
	RecordingCount  int    `json:"recordingCount"`
	// Freshness (match#36 / #9): is the backup actually tracking the leader?
	RecordingLogBytes  int64            `json:"recordingLogBytes"`
	CatalogModifiedAgo int64            `json:"catalogModifiedAgoSec"` // -1 when absent
	Heartbeat          *BackupHeartbeat `json:"heartbeat,omitempty"`
	Fresh              bool             `json:"fresh"`
	FreshReason        string           `json:"freshReason"`
}

// BackupHeartbeat mirrors backup-progress.json written by ClusterBackupApp's
// watchdog (match-cluster) every 5s next to the backup data.
type BackupHeartbeat struct {
	Pid                 int64  `json:"pid"`
	StartedEpochMs      int64  `json:"startedEpochMs"`
	UpdatedEpochMs      int64  `json:"updatedEpochMs"`
	LastProgressEpochMs int64  `json:"lastProgressEpochMs"`
	LastQueryEpochMs    int64  `json:"lastQueryEpochMs"`
	LastResponseEpochMs int64  `json:"lastResponseEpochMs"`
	LastLiveLogEpochMs  int64  `json:"lastLiveLogEpochMs"`
	LiveLogPosition     int64  `json:"liveLogPosition"`
	SnapshotsRetrieved  int64  `json:"snapshotsRetrieved"`
	StallWarnings       int64  `json:"stallWarnings"`
	State               string `json:"state"`
}

// heartbeat must be at most this old to count as live (watchdog writes every 5s)
const backupHeartbeatMaxAgeSec = 30

// BackupFreshness reads the ClusterBackupApp heartbeat and derives whether the
// backup is live and tracking the leader. A "running" backup process proves
// nothing (match#36: the agent wedged silently for days while the process
// looked healthy) — only a recent heartbeat in state OK does.
func BackupFreshness(backupDir string) (fresh bool, reason string, hb *BackupHeartbeat) {
	data, err := os.ReadFile(filepath.Join(backupDir, "backup-progress.json"))
	if err != nil {
		return false, "no heartbeat file (backup app not running, or predates the watchdog)", nil
	}
	hb = &BackupHeartbeat{}
	if err := json.Unmarshal(data, hb); err != nil {
		return false, "unreadable heartbeat: " + err.Error(), nil
	}
	ageSec := (time.Now().UnixMilli() - hb.UpdatedEpochMs) / 1000
	if ageSec > backupHeartbeatMaxAgeSec {
		return false, fmt.Sprintf("heartbeat stale: last written %ds ago (backup process dead or wedged)", ageSec), hb
	}
	if hb.State != "OK" {
		return false, fmt.Sprintf("backup reports state %s (no progress; watchdog about to restart it)", hb.State), hb
	}
	return true, "heartbeat live, backup making progress", hb
}

// GetBackupInfo returns information about backup data availability
func (o *OperationsService) GetBackupInfo() BackupInfo {
	backupDir := o.cluster.BackupDir
	info := BackupInfo{BackupDir: backupDir, CatalogModifiedAgo: -1}

	if st, err := os.Stat(filepath.Join(backupDir, "cluster/recording.log")); err == nil {
		info.HasRecordingLog = true
		info.RecordingLogBytes = st.Size()
	}
	if st, err := os.Stat(filepath.Join(backupDir, "archive/archive.catalog")); err == nil {
		info.HasArchive = true
		info.CatalogModifiedAgo = int64(time.Since(st.ModTime()).Seconds())
	}
	matches, _ := filepath.Glob(filepath.Join(backupDir, "archive/*.rec"))
	info.RecordingCount = len(matches)

	info.Fresh, info.FreshReason, info.Heartbeat = BackupFreshness(backupDir)

	return info
}

// RecoverFromBackup restores a node's cluster data from the backup directory
func (o *OperationsService) RecoverFromBackup(nodeId int, force, dryRun bool) map[string]interface{} {
	result := map[string]interface{}{"nodeId": nodeId}

	if nodeId < 0 || nodeId >= o.cluster.NodeCount() {
		result["success"] = false
		result["error"] = fmt.Sprintf("Invalid nodeId (must be 0..%d)", o.cluster.NodeCount()-1)
		return result
	}

	if !force {
		result["success"] = false
		result["error"] = "Destructive operation requires force=true"
		return result
	}

	if o.isNodeRunning(nodeId) {
		result["success"] = false
		result["error"] = fmt.Sprintf("Node %d must be stopped before recovery", nodeId)
		return result
	}

	backupDir := o.cluster.BackupDir
	nodeDir := o.cluster.NodeStateDir(nodeId)

	// Check backup exists
	if _, err := os.Stat(filepath.Join(backupDir, "archive/archive.catalog")); os.IsNotExist(err) {
		result["success"] = false
		result["error"] = "No backup found at " + backupDir + "/archive/archive.catalog"
		return result
	}

	if dryRun {
		result["success"] = true
		result["dryRun"] = true
		result["source"] = backupDir
		result["target"] = nodeDir
		return result
	}

	// Create directories
	os.MkdirAll(filepath.Join(nodeDir, "cluster"), 0755)
	os.MkdirAll(filepath.Join(nodeDir, "archive"), 0755)

	// Copy archive catalog and recordings
	if err := copyFile(filepath.Join(backupDir, "archive/archive.catalog"),
		filepath.Join(nodeDir, "archive/archive.catalog")); err != nil {
		result["success"] = false
		result["error"] = "Failed to copy archive.catalog: " + err.Error()
		return result
	}

	recFiles, _ := filepath.Glob(filepath.Join(backupDir, "archive/*.rec"))
	for _, src := range recFiles {
		if err := copyFile(src, filepath.Join(nodeDir, "archive", filepath.Base(src))); err != nil {
			result["success"] = false
			result["error"] = "Failed to copy " + filepath.Base(src) + ": " + err.Error()
			return result
		}
	}

	// Copy recording.log if exists
	recordingLogSrc := filepath.Join(backupDir, "cluster/recording.log")
	if _, err := os.Stat(recordingLogSrc); err == nil {
		copyFile(recordingLogSrc, filepath.Join(nodeDir, "cluster/recording.log"))
	}

	// Seed from snapshot
	output, err := o.cluster.SeedRecordingLogFromSnapshot(nodeId)
	if err != nil {
		result["success"] = false
		result["error"] = "SeedRecordingLogFromSnapshot failed: " + err.Error()
		result["output"] = output
		return result
	}

	result["success"] = true
	result["message"] = fmt.Sprintf("Node %d recovered from backup", nodeId)
	result["recordingsCopied"] = len(recFiles)
	return result
}

// backupMarkFiles creates a timestamped backup of mark files before cleanup
func (o *OperationsService) backupMarkFiles() string {
	timestamp := time.Now().Format("20060102-150405")
	backupDir := filepath.Join(o.cluster.BackupDir, "pre-cleanup", timestamp)
	os.MkdirAll(backupDir, 0755)

	for i := 0; i < o.cluster.NodeCount(); i++ {
		nodeDir := o.cluster.NodeStateDir(i)
		nodeBackup := filepath.Join(backupDir, o.cluster.NodeName(i))
		os.MkdirAll(nodeBackup, 0755)

		files := []string{"cluster/cluster-mark.dat", "cluster/recording.log",
			"archive/archive-mark.dat", "archive/archive.catalog"}
		for _, f := range files {
			src := filepath.Join(nodeDir, f)
			if _, err := os.Stat(src); err == nil {
				copyFile(src, filepath.Join(nodeBackup, filepath.Base(f)))
			}
		}
	}
	return backupDir
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// waitForPort polls until a UDP port is open (bound) on the given host
func (o *OperationsService) waitForPort(host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("udp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			// UDP "connect" always succeeds — check if port is actually bound via ss
			if o.isPortOpen(host, port) {
				return true
			}
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

// isPortOpen checks if a UDP port is bound using ss
func (o *OperationsService) isPortOpen(host string, port int) bool {
	cmd := exec.Command("ss", "-ulnp")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	target := fmt.Sprintf("%s:%d", host, port)
	return strings.Contains(string(output), target)
}

// waitForNodeStopped waits until a node's process is no longer running
func (o *OperationsService) waitForNodeStopped(log *slog.Logger, nodeId int, timeout time.Duration) {
	service := o.cluster.NodeName(nodeId)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !o.isNodeRunning(nodeId) {
			return
		}
		time.Sleep(1 * time.Second)
	}
	// Force kill if still running
	log.Warn("node still running after stop timeout, force killing", "node", nodeId)
	if o.procMgr != nil {
		info := o.procMgr.Get(service)
		if info != nil && info.PID > 0 {
			exec.Command("kill", "-9", fmt.Sprintf("%d", info.PID)).Run()
			time.Sleep(1 * time.Second)
		}
	}
}

// cleanNodeMediaDriver removes stale Aeron MediaDriver directories for a node.
// Never touches the dir while an external media driver (driverN) owns it — in external
// mode the driver process survives node restarts and deleting its dir corrupts the IPC.
// Ownership is judged by tracked state AND the launch script's pid file: tracked
// state reads stopped during driver crash-loops and adoption gaps, which is how
// the 2026-07-06 rolling update deleted node0's live dir (#42).
func (o *OperationsService) cleanNodeMediaDriver(log *slog.Logger, nodeId int) {
	trackedRunning := false
	nodeRunning := false
	if o.procMgr != nil {
		if info := o.procMgr.Get(o.cluster.DriverName(nodeId)); info != nil && info.Running {
			trackedRunning = true
		}
		// Embedded-mode fallback (ag#68): the owning node's liveness blocks the
		// delete when there is no external driver pid file to trust.
		if info := o.procMgr.Get(o.cluster.NodeName(nodeId)); info != nil && info.Running {
			nodeRunning = true
		}
	}
	driverDir := o.cluster.DriverAeronDir(nodeId)
	ok, reason := canDeleteDriverDir(driverDir, trackedRunning, nodeRunning)
	if !ok {
		if !trackedRunning {
			log.Error("refusing to delete media driver dir (#42 guard)", "dir", driverDir, "reason", reason)
		}
		return
	}
	if _, err := os.Stat(driverDir); err == nil {
		os.RemoveAll(driverDir)
		log.Info("cleaned stale media driver dir", "dir", driverDir)
	}
}

// waitForFollowerCatchUp blocks until the follower's cluster commit position is within
// catchUpLagBytes of the leader's commit position, OR the timeout elapses. Returns true if
// caught up, false on timeout. Uses the CnC counters (no JVM spawn) so it's cheap to poll.
//
// Why this matters: rolling update used to advance to the next node as soon as the previous
// follower's ingress port was open. The node was up but might still be replaying the log or
// loading a snapshot. Restarting the next node before catch-up risks losing quorum.
func (o *OperationsService) waitForFollowerCatchUp(log *slog.Logger, followerId, leaderId int, timeout time.Duration) bool {
	const catchUpLagBytes int64 = 1 * 1024 * 1024 // 1 MB lag is fine; term buffer is 16 MB
	deadline := time.Now().Add(timeout)
	var lastFollowerPos int64 = -1
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		// Per-node counter reads go through the agent; the cross-node
		// comparison stays control-plane-side (docs/AGENT-ARCHITECTURE.md).
		leader, lerr := o.procMgr.NodeCounters(leaderId)
		follower, ferr := o.procMgr.NodeCounters(followerId)
		if lerr != nil || ferr != nil || leader.CommitPosition < 0 || follower.CommitPosition < 0 {
			continue // CnC not yet ready; keep polling
		}
		lag := leader.CommitPosition - follower.CommitPosition
		if lag <= catchUpLagBytes {
			log.Info("node caught up to leader", "node", followerId, "lag_bytes", lag)
			return true
		}
		if follower.CommitPosition <= lastFollowerPos && follower.CommitPosition > 0 {
			// Position not advancing — log but keep waiting until timeout.
			log.Warn("node catch-up stalled", "node", followerId, "pos", follower.CommitPosition, "lag_bytes", lag)
		}
		lastFollowerPos = follower.CommitPosition
	}
	return false
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
