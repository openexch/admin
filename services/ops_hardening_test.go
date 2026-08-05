// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/logging"
)

// ag#67 Fix 1: a panic in an op goroutine must be CONTAINED (an unrecovered
// panic in any goroutine is otherwise fatal to the whole admin gateway),
// recorded to a durable, queryable failure record, and the Progress slot freed
// with the error — before this the failure vanished with the goroutine and the
// slot could wedge every future operation.
func TestOpPanicWritesDurableFailureAndFinishes(t *testing.T) {
	dir := t.TempDir()
	o := &OperationsService{cfg: &config.Config{AdminDir: dir}, progress: NewProgress(), log: logging.Component("test")}
	if !o.progress.TryStart("rolling-update", 11) {
		t.Fatal("could not claim the progress slot")
	}

	// Exercise the exact defer every doX goroutine now carries.
	func() {
		defer o.recoverOp()
		panic("boom in the op goroutine")
	}()

	m := o.progress.ToMap()
	if m["complete"] != true || m["error"] != true {
		t.Fatalf("a panic must Finish the slot with error, got %v", m)
	}
	if o.progress.IsRunning() {
		t.Fatal("the slot must be freed after the panic, not left wedged")
	}

	rec := ReadOpFailure(dir)
	if rec["state"] != "failed" || rec["op"] != "rolling-update" || rec["stage"] != "panic" {
		t.Fatalf("expected a durable panic record for rolling-update, got %v", rec)
	}
	if !strings.Contains(fmt.Sprint(rec["reason"]), "boom") {
		t.Fatalf("record must carry the panic reason, got %v", rec["reason"])
	}
	if o.LastOpFailure()["op"] != "rolling-update" {
		t.Fatalf("LastOpFailure must surface the same record, got %v", o.LastOpFailure())
	}

	// No panic → recover() is nil → recoverOp is a no-op (happy path unchanged).
	o.progress.TryStart("snapshot", 3)
	func() { defer o.recoverOp() }()
	if o.progress.IsRunning() == false && o.progress.ToMap()["error"] == true {
		t.Fatal("a clean return must not be recorded as a failure")
	}
}

// ag#68 Fix 2: cleanupSweep must refuse to delete an embedded-driver dir whose
// owning node is tracked-running OR whose cnc.dat is fresh, even though there is
// no <dir>.pid file for the pid-file guard to consult — the vacuous-guard hole
// that made cleanupSweep the last unguarded /dev/shm driver-dir deleter.
func TestCleanupSweepGuardRefusesLiveEmbeddedDriverDir(t *testing.T) {
	cfg := &config.Config{AssetsStateDir: "/dev/shm/aeron-assets"}
	assets := NewAssetsCluster(cfg) // embedded driver, node "ae0", DriverName ""
	fa := newFakeAgent()
	o := &OperationsService{cfg: cfg, cluster: assets, progress: NewProgress(), log: logging.Component("test")}
	o.SetProcessManager(fa)

	shm, tmp := t.TempDir(), t.TempDir()
	driverBase := filepath.Base(assets.DriverAeronDir(0)) // aeron-<user>-assets-0-driver
	driverDir := filepath.Join(shm, driverBase)
	if err := os.MkdirAll(driverDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cnc := filepath.Join(driverDir, "cnc.dat")
	if err := os.WriteFile(cnc, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Deliberately NO <driverDir>.pid file: embedded-driver mode.
	const stateBase = "aeron-assets"

	// 1) ae0 tracked-running: the live driver dir must survive the sweep.
	fa.set(agent.ProcessInfo{Name: "ae0", Running: true})
	_, _, errs, refused := cleanupSweep(shm, tmp, stateBase, nil, o.driverDirGuard(), false, true)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if !exists(driverDir) {
		t.Fatal("the live embedded driver dir must NOT be deleted while ae0 runs")
	}
	if len(refused) != 1 || !strings.Contains(refused[0], driverBase) {
		t.Fatalf("the refused live driver dir must be reported, got %v", refused)
	}

	// 2) ae0 down but cnc.dat freshly written: still refused (no pid file to trust).
	fa.set(agent.ProcessInfo{Name: "ae0", Running: false})
	if _, _, _, refused = cleanupSweep(shm, tmp, stateBase, nil, o.driverDirGuard(), false, true); len(refused) != 1 {
		t.Fatalf("a fresh cnc.dat must still block deletion, got refused=%v", refused)
	}
	if !exists(driverDir) {
		t.Fatal("a fresh cnc.dat must keep the dir even with the node down")
	}

	// 3) Age cnc.dat past the freshness window with the node down: now reclaimable.
	old := time.Now().Add(-2 * driverCncFreshWindow)
	if err := os.Chtimes(cnc, old, old); err != nil {
		t.Fatal(err)
	}
	if _, _, _, refused = cleanupSweep(shm, tmp, stateBase, nil, o.driverDirGuard(), false, true); len(refused) != 0 {
		t.Fatalf("a stale, unowned driver dir must not be refused, got %v", refused)
	}
	if exists(driverDir) {
		t.Fatal("a stale, unowned embedded driver dir should be reclaimed")
	}
}

// ag#83 Fix 3: the assets cluster's OperationsService is now wired to preflight,
// so a failing GLOBAL check (mem-available) blocks an assets op — while the
// match-SPECIFIC checks (cluster-quorum over the 3-node ME) are scoped out and
// never gate a single-node assets op.
func TestAssetsOpsGateConsultsPreflightScoped(t *testing.T) {
	cfg := &config.Config{MinMemMB: 4096, MinRootDiskGB: 0, MaxShmUsedPct: 100, AssetsStateDir: "/dev/shm/aeron-assets"}
	assets := NewAssetsCluster(cfg)
	o := &OperationsService{cfg: cfg, cluster: assets, progress: NewProgress(), log: logging.Component("test")}
	p := newTestPreflight(cfg)
	// The matching engine is fully dead: its quorum check would block if applied.
	p.status = &fakeStatus{statusWithHealth(HealthDead, HealthDead, HealthDead)}
	o.SetPreflight(p)

	// Low memory (a global check) blocks the assets op, naming mem-available and
	// NOT the match-specific quorum, and frees the slot on refusal.
	p.meminfoPath = writeMeminfo(t, 1000)
	o.progress.TryStart("rolling-update", 11)
	err := o.gate("rolling-update", false)
	if err == nil || !strings.Contains(err.Error(), "mem-available") {
		t.Fatalf("assets rolling-update must be gated by the global mem check, got %v", err)
	}
	if strings.Contains(err.Error(), "cluster-quorum") {
		t.Fatalf("assets op must NOT be gated by the match-specific quorum check, got %v", err)
	}
	if o.progress.IsRunning() {
		t.Fatal("a gate refusal must free the progress slot")
	}

	// With memory fine the assets gate PASSES despite the ME being dead — the
	// quorum/driver-dirs checks are scoped out for the assets cluster.
	p.meminfoPath = writeMeminfo(t, 8192)
	o.progress.TryStart("rolling-update", 11)
	if err := o.gate("rolling-update", false); err != nil {
		t.Fatalf("assets gate should pass when globals are fine (ME checks scoped out), got %v", err)
	}
}
