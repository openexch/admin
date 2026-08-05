// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/openexch/admin/logging"
)

// AutoSnapshot manages periodic snapshot scheduling for EVERY cluster.
//
// It used to hold a single OperationsService — the matching engine's. When the
// Assets Engine became a first-class cluster the HTTP handlers learned to route
// per-cluster (opsFor / clusterOps) but this scheduler did not, so the AE was
// never auto-snapshotted at all and its cluster log grew without bound from the
// day it shipped. That is what filled /dev/shm and killed the money path on
// 2026-07-25: no snapshot means no log truncation, tmpfs fills, the archive
// starts failing writes with "No space left on device", and nodes terminate.
// A bigger box only moves that day further out; it does not remove it.
type AutoSnapshot struct {
	mu              sync.RWMutex
	enabled         bool
	intervalMinutes int64
	byteThreshold   int64
	stopChan        chan struct{}
	targets         []snapshotTarget
	results         map[string]*snapshotResult
	lastTrigger     map[string]string
	clocks          map[string]*cadenceClock
	log             *slog.Logger

	// archive is the durable record, consulted when the in-memory one is empty.
	// Nil leaves the old behaviour exactly as it was.
	archive LedgerArchiver
}

// SetLedgerArchive gives the reporter access to evidence that outlives the
// process.
//
// The success counters here are deliberately in-memory: seeding them from disk
// would let a restarted gateway claim a snapshot it never took. The cost is that
// a restart reports "never" for a cluster that has been snapshotting all week,
// and that cost used to be small because a directory listing of local bundles
// settled the question. Reclaiming those bundles as soon as S3 confirms them
// removed that second record, so this one replaces it — from S3, which is where
// the durability is, rather than from a file that could disagree with it.
// archiver is the read side: a zero-value AutoSnapshot (tests, a literal) has a
// nil interface here, and the reporting path must not be the place that panics.
func (a *AutoSnapshot) archiver() LedgerArchiver {
	if a.archive == nil {
		return NoLedgerArchive
	}
	return a.archive
}

func (a *AutoSnapshot) SetLedgerArchive(archive LedgerArchiver) {
	if archive == nil {
		archive = NoLedgerArchive
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.archive = archive
}

// A fixed wall-clock interval silently caps the rate at which the engine can
// run in memory: between two snapshots the log has to fit in tmpfs, and at
// 400k/s five minutes is ~13.5 GB against a 7.8 GB /dev/shm. The byte trigger
// is what holds the ledger-stays-in-RAM decision at ANY throughput; the timer
// is only a floor for quiet markets, so a slow day still produces recent
// restore points.
//
// It pays a second time at the other end of the range. At demo rates a
// five-minute cadence produces bundles that re-carry the same partially filled
// 64 MB segment - about 6 MB uploaded to convey 370 KB of new log. Snapshotting
// by bytes makes each bundle a predictable size instead.
const (
	defaultSnapshotByteThreshold int64 = 1 << 30 // 1 GiB
	// How often the trigger is evaluated. Cheap: it reads the status cache the
	// console already polls, and spawns nothing.
	snapshotEvaluateInterval = 15 * time.Second
)

type snapshotTarget struct {
	name string
	ops  *OperationsService
}

// snapshotResult is per-cluster history. A snapshot that is refused every cycle
// is indistinguishable from one that never runs unless the outcome is recorded,
// and "silently stopped doing the work" is exactly how 2026-07-25 stayed
// invisible for sixteen hours.
type snapshotResult struct {
	Successes        int
	LastSuccessAt    time.Time
	LastFailureAt    time.Time
	LastError        string
	ConsecutiveFails int
}

// cadenceClock is when the interval is measured FROM, kept separate from
// LastSuccessAt on purpose.
//
// They were briefly the same field, and the status immediately started
// reporting "snapshotted 2 minutes ago" for a cluster that had never
// snapshotted at all: seeding the clock at startup wrote a success that did not
// happen. A restore point that does not exist is precisely the signal this
// whole epic exists to stop inventing.
type cadenceClock struct {
	since time.Time
}

// NewAutoSnapshot builds a scheduler over every cluster it is given. Pass the
// same map the HTTP layer routes with, so a cluster can never be reachable by
// hand but invisible to the scheduler.
func NewAutoSnapshot(clusterOps map[string]*OperationsService) *AutoSnapshot {
	names := make([]string, 0, len(clusterOps))
	for name := range clusterOps {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic cycle order

	targets := make([]snapshotTarget, 0, len(names))
	for _, name := range names {
		targets = append(targets, snapshotTarget{name: name, ops: clusterOps[name]})
	}
	return &AutoSnapshot{
		targets:       targets,
		archive:       NoLedgerArchive,
		results:       make(map[string]*snapshotResult, len(targets)),
		lastTrigger:   make(map[string]string, len(targets)),
		clocks:        make(map[string]*cadenceClock, len(targets)),
		byteThreshold: defaultSnapshotByteThreshold,
		log:           logging.Component("autosnapshot"),
	}
}

// Start begins periodic snapshots at the given interval.
func (a *AutoSnapshot) Start(intervalMinutes int64) {
	a.Stop() // Stop existing scheduler

	a.mu.Lock()
	a.enabled = true
	a.intervalMinutes = intervalMinutes
	a.stopChan = make(chan struct{})
	stop := a.stopChan
	names := make([]string, 0, len(a.targets))
	now := time.Now()
	for _, t := range a.targets {
		names = append(names, t.name)
		// Seed the CLOCK, never the success record, so a gateway restart does
		// not fire an immediate snapshot on every cluster and does not claim a
		// snapshot it never took. A busy cluster still fires at once, on bytes,
		// which is the case that actually matters.
		a.clocks[t.name] = &cadenceClock{since: now}
	}
	threshold := a.byteThreshold
	a.mu.Unlock()

	go func() {
		ticker := time.NewTicker(snapshotEvaluateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				a.runSnapshotCycle(stop)
			case <-stop:
				return
			}
		}
	}()

	a.log.Info("auto-snapshot enabled", "interval_minutes", intervalMinutes,
		"byte_threshold", threshold, "clusters", names)
}

// Stop disables periodic snapshots.
func (a *AutoSnapshot) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stopChan != nil {
		close(a.stopChan)
		a.stopChan = nil
	}
	a.enabled = false
	a.intervalMinutes = 0
}

// IsEnabled returns whether auto-snapshot is active.
func (a *AutoSnapshot) IsEnabled() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.enabled
}

// GetIntervalMinutes returns the current interval.
func (a *AutoSnapshot) GetIntervalMinutes() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.intervalMinutes
}

// runSnapshotCycle snapshots every cluster, one at a time.
//
// Sequential by necessity: the clusters share one operation slot, so firing
// them together would make each cycle a race in which one cluster wins and the
// other reports "another operation in progress" forever. A cluster that fails
// or is refused does NOT stop the cycle — the whole point is that an unhealthy
// matching engine must not also stop the Assets Engine from snapshotting.
//
// The snapshot path reclaims archive disk via the live-safe ArchiveHousekeeping
// (purges log segments below the snapshot position) on each node, so no separate
// compaction step is needed. Aeron's offline ArchiveTool compaction is
// deliberately NOT run here: against a live node it corrupts the latest snapshot
// recording and breaks recover-from-snapshot (nodes crash on restart with
// "unknown recording id" and the cluster comes up unable to serve ingress).
func (a *AutoSnapshot) runSnapshotCycle(stop <-chan struct{}) {
	a.mu.RLock()
	targets := make([]snapshotTarget, len(a.targets))
	copy(targets, a.targets)
	a.mu.RUnlock()

	for _, t := range targets {
		select {
		case <-stop:
			return
		default:
		}
		if reason := a.dueReason(t); reason != "" {
			a.noteTrigger(t.name, reason)
			a.snapshotOne(t, stop)
		}
	}
}

// dueReason decides whether a cluster should snapshot now, and says why.
//
// Bytes first: it is the condition that keeps the ledger in RAM, and reporting
// "timer" for a snapshot the log size forced would hide the thing an operator
// needs to see when throughput climbs.
//
// Returns "" when the cluster is not due. An unreadable log size is NOT treated
// as zero — it falls through to the timer, so a broken status reading degrades
// to the old fixed cadence instead of silently never snapshotting.
func (a *AutoSnapshot) dueReason(t snapshotTarget) string {
	a.mu.RLock()
	threshold := a.byteThreshold
	interval := time.Duration(a.intervalMinutes) * time.Minute
	var last time.Time
	if c, ok := a.clocks[t.name]; ok {
		last = c.since
	}
	a.mu.RUnlock()

	if bytes := t.ops.LogBytesSinceSnapshot(); bytes >= 0 && threshold > 0 && bytes >= threshold {
		return fmt.Sprintf("log grew %d bytes since the last snapshot (threshold %d)",
			bytes, threshold)
	}

	if interval > 0 && last.IsZero() {
		return "no snapshot taken yet"
	}
	if interval > 0 && time.Since(last) >= interval {
		return fmt.Sprintf("%s since the last snapshot (interval %s)",
			time.Since(last).Truncate(time.Second), interval)
	}

	return ""
}

// noteTrigger records why the last cycle fired, so /status can distinguish a
// cluster snapshotting on volume from one merely ticking over.
func (a *AutoSnapshot) noteTrigger(cluster, reason string) {
	a.mu.Lock()
	a.lastTrigger[cluster] = reason
	// Advance the clock on the ATTEMPT, not on success. A cluster that cannot
	// snapshot would otherwise retry every evaluation tick and drown the log;
	// its failure count is what surfaces the problem, and a growing log still
	// forces a retry through the byte trigger regardless.
	if c, ok := a.clocks[cluster]; ok {
		c.since = time.Now()
	} else {
		a.clocks[cluster] = &cadenceClock{since: time.Now()}
	}
	a.mu.Unlock()
	a.log.Info("snapshot due", "cluster", cluster, "reason", reason)
}

func (a *AutoSnapshot) snapshotOne(t snapshotTarget, stop <-chan struct{}) {
	log := a.log.With("cluster", t.name)

	if t.ops.progress.IsRunning() {
		a.recordFailure(t.name, "another operation in progress")
		log.Warn("snapshot skipped: another operation in progress")
		return
	}

	// Never forced: an unhealthy/lagging member makes snapshotting dangerous
	// (match#35) — skipping a cycle is safe, stranding a member is not. But a
	// cluster stuck refusing every cycle is NOT self-healing, so the refusal is
	// counted and surfaced rather than just logged and forgotten.
	if err := t.ops.Snapshot(false); err != nil {
		a.recordFailure(t.name, err.Error())
		fails := a.consecutiveFails(t.name)
		// One refusal is routine. A cluster that has not snapshotted in an hour
		// is on its way to an unbounded log and a full disk.
		if fails >= 12 {
			log.Error("cluster has not snapshotted for many cycles — its log is growing unbounded",
				"consecutive_failures", fails, "err", err)
		} else {
			log.Warn("snapshot skipped", "consecutive_failures", fails, "err", err)
		}
		return
	}

	if !a.waitForOperation(t.ops, 5*time.Minute, stop) {
		a.recordFailure(t.name, "snapshot did not complete in time")
		log.Warn("snapshot did not complete in time")
		return
	}

	a.recordSuccess(t.name)
	log.Info("snapshot complete")
}

// waitForOperation polls until the given cluster's operation finishes or timeout.
func (a *AutoSnapshot) waitForOperation(ops *OperationsService, timeout time.Duration, stop <-chan struct{}) bool {
	deadline := time.After(timeout)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-deadline:
			return false
		case <-tick.C:
			if !ops.progress.IsRunning() {
				return true
			}
		case <-stop:
			return false
		}
	}
}

func (a *AutoSnapshot) resultFor(cluster string) *snapshotResult {
	r, ok := a.results[cluster]
	if !ok {
		r = &snapshotResult{}
		a.results[cluster] = r
	}
	return r
}

func (a *AutoSnapshot) recordSuccess(cluster string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.resultFor(cluster)
	r.Successes++
	r.LastSuccessAt = time.Now()
	r.ConsecutiveFails = 0
	r.LastError = ""
}

func (a *AutoSnapshot) recordFailure(cluster, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.resultFor(cluster)
	r.LastFailureAt = time.Now()
	r.LastError = reason
	r.ConsecutiveFails++
}

func (a *AutoSnapshot) consecutiveFails(cluster string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if r, ok := a.results[cluster]; ok {
		return r.ConsecutiveFails
	}
	return 0
}

// ToMap returns status as a map, including per-cluster outcomes so an operator
// (and /metrics) can see that snapshots are actually HAPPENING rather than just
// that the scheduler is switched on.
func (a *AutoSnapshot) ToMap() map[string]interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()

	clusters := make(map[string]interface{}, len(a.targets))
	for _, t := range a.targets {
		entry := map[string]interface{}{"snapshots": 0, "consecutiveFailures": 0}
		if reason, ok := a.lastTrigger[t.name]; ok && reason != "" {
			entry["lastTrigger"] = reason
		}
		if bytes := t.ops.LogBytesSinceSnapshot(); bytes >= 0 {
			entry["logBytesSinceSnapshot"] = bytes
		}
		if r, ok := a.results[t.name]; ok {
			entry["snapshots"] = r.Successes
			entry["consecutiveFailures"] = r.ConsecutiveFails
			if !r.LastSuccessAt.IsZero() {
				entry["lastSuccess"] = r.LastSuccessAt.UTC().Format(time.RFC3339)
				entry["secondsSinceLastSuccess"] = int64(time.Since(r.LastSuccessAt).Seconds())
			}
			if r.LastError != "" {
				entry["lastError"] = r.LastError
			}
		}

		// Reported ALONGSIDE the counters, never merged into them. "This process
		// has taken 0 snapshots" and "S3 holds one from four minutes ago" are
		// different facts, and folding the second into the first would be the
		// gateway claiming credit for work it cannot remember doing — the exact
		// thing keeping the counters in memory was meant to prevent.
		if at := a.archiver().DurableSnapshotAt(t.name); !at.IsZero() {
			entry["durableSnapshotAt"] = at.UTC().Format(time.RFC3339)
			entry["secondsSinceDurableSnapshot"] = int64(time.Since(at).Seconds())
		}
		clusters[t.name] = entry
	}

	return map[string]interface{}{
		"enabled":         a.enabled,
		"intervalMinutes": a.intervalMinutes,
		"byteThreshold":   a.byteThreshold,
		"clusters":        clusters,
	}
}
