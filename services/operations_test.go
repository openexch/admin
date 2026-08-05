// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeHeartbeat(t *testing.T, dir string, ageSec int64, state string) {
	t.Helper()
	now := time.Now().UnixMilli() - ageSec*1000
	json := fmt.Sprintf(`{"pid":1,"startedEpochMs":%d,"updatedEpochMs":%d,"lastProgressEpochMs":%d,`+
		`"lastQueryEpochMs":%d,"lastResponseEpochMs":%d,"lastLiveLogEpochMs":%d,`+
		`"liveLogPosition":42,"snapshotsRetrieved":1,"stallWarnings":0,"state":"%s"}`,
		now, now, now, now, now, now, state)
	if err := os.WriteFile(filepath.Join(dir, "backup-progress.json"), []byte(json), 0644); err != nil {
		t.Fatal(err)
	}
}

// cleanupFixture builds a fake /dev/shm + /tmp layout with driver/client IPC
// dirs, mark/lock files, and cluster archives.
func cleanupFixture(t *testing.T) (shm, tmp string) {
	t.Helper()
	shm, tmp = t.TempDir(), t.TempDir()
	mk := func(path string) {
		t.Helper()
		full := filepath.Join(shm, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	mk("aeron-emre-0-driver/cnc.dat")
	mk("aeron-market-1234/cnc.dat")
	mk("aeron-cluster/node0/cluster/cluster-mark.dat")
	mk("aeron-cluster/node0/cluster/election.lck")
	mk("aeron-cluster/node0/archive/archive-mark.dat")
	mk("aeron-cluster/node0/archive/0-0.rec")
	mk("aeron-cluster/node0/archive/archive.catalog")
	mk("aeron-cluster/node1/archive/5-0.rec")
	mk("aeron-cluster/node1/cluster/recording.log")
	if err := os.MkdirAll(filepath.Join(tmp, "aeron-gw-99"), 0755); err != nil {
		t.Fatal(err)
	}
	return shm, tmp
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// #10 exit criterion: /cleanup provably preserves archives.
func TestCleanupSweepPreservesArchives(t *testing.T) {
	shm, tmp := cleanupFixture(t)

	_, preserved, errs, _ := cleanupSweep(shm, tmp, filepath.Join(shm, "aeron-cluster"), nil, nil, false, true)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	// IPC dirs, mark files, locks: gone
	for _, gone := range []string{
		"aeron-emre-0-driver", "aeron-market-1234",
		"aeron-cluster/node0/cluster/cluster-mark.dat",
		"aeron-cluster/node0/cluster/election.lck",
		"aeron-cluster/node0/archive/archive-mark.dat",
	} {
		if exists(filepath.Join(shm, gone)) {
			t.Fatalf("%s should have been cleaned", gone)
		}
	}
	if exists(filepath.Join(tmp, "aeron-gw-99")) {
		t.Fatal("tmp aeron dir should have been cleaned")
	}

	// Archives and cluster recovery state: PRESERVED
	for _, kept := range []string{
		"aeron-cluster/node0/archive/0-0.rec",
		"aeron-cluster/node0/archive/archive.catalog",
		"aeron-cluster/node1/archive/5-0.rec",
		"aeron-cluster/node1/cluster/recording.log",
	} {
		if !exists(filepath.Join(shm, kept)) {
			t.Fatalf("%s must be preserved by default cleanup", kept)
		}
	}
	if len(preserved) == 0 || !strings.Contains(preserved[0], "2 archive recording(s)") {
		t.Fatalf("preserved notice missing/wrong: %v", preserved)
	}
}

func TestCleanupSweepIncludeArchiveWipes(t *testing.T) {
	shm, tmp := cleanupFixture(t)

	_, preserved, errs, _ := cleanupSweep(shm, tmp, filepath.Join(shm, "aeron-cluster"), nil, nil, true, true)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if exists(filepath.Join(shm, "aeron-cluster")) {
		t.Fatal("includeArchive should wipe the whole cluster state dir")
	}
	if len(preserved) != 0 {
		t.Fatalf("nothing should be reported preserved on a full wipe: %v", preserved)
	}
}

// Regression for the 2026-07-15 bridge-halt incident: when the ME state root is
// on disk (env MATCH_STATE_DIR), NOT under /dev/shm, the sweep must still wipe
// its archives. The old code derived the archive path from shmDir/<basename> and
// silently missed the disk state — a stale ME snapshot survived every wipe,
// restored at a tradeId mismatched with the genesis AE, and HALTED the
// settlement bridge (no money settled, all holds leaked).
func TestCleanupSweepWipesOffShmStateRoot(t *testing.T) {
	shm, tmp := t.TempDir(), t.TempDir()
	// A driver IPC dir lives under /dev/shm...
	if err := os.MkdirAll(filepath.Join(shm, "aeron-emre-0-driver"), 0755); err != nil {
		t.Fatal(err)
	}
	// ...but the ME cluster state+archive lives on disk elsewhere (MATCH_STATE_DIR).
	stateRoot := filepath.Join(t.TempDir(), "aeron-cluster")
	for _, rel := range []string{"node0/archive/0-0.rec", "node0/cluster/cluster-mark.dat", "node1/archive/5-0.rec"} {
		full := filepath.Join(stateRoot, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	if _, _, errs, _ := cleanupSweep(shm, tmp, stateRoot, nil, nil, true, true); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if exists(stateRoot) {
		t.Fatal("includeArchive must wipe the disk-based ME state root (the bridge-halt bug)")
	}
	// The /dev/shm IPC dir is still swept.
	if exists(filepath.Join(shm, "aeron-emre-0-driver")) {
		t.Fatal("driver IPC dir under /dev/shm should still be cleaned")
	}
}

// Multi-cluster scoping: one cluster's sweep must never touch another
// cluster's state or driver dirs (the 2026-07-09 clean-slate wiped the assets
// engine's dirs under a live ae0 before this existed), and the assets sweep
// must clean ONLY its own.
func TestCleanupSweepScopedPerCluster(t *testing.T) {
	shm, tmp := cleanupFixture(t)
	mk := func(rel string) {
		path := filepath.Join(shm, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// The assets engine's dirs, live on the same box.
	mk("aeron-assets/ae0/cluster/cluster-mark.dat")
	mk("aeron-assets/ae0/archive/0-0.rec")
	mk("aeron-emre-assets-0-driver/cnc.dat")

	assetsDirs := map[string]bool{"aeron-assets": true, "aeron-emre-assets-0-driver": true}
	matchDirs := map[string]bool{
		"aeron-cluster": true, "aeron-emre-0-driver": true,
		"aeron-emre-1-driver": true, "aeron-emre-2-driver": true,
	}

	// Match cleanup (full wipe) with the assets dirs excluded: assets untouched.
	if _, _, errs, _ := cleanupSweep(shm, tmp, filepath.Join(shm, "aeron-cluster"), assetsDirs, nil, true, true); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if exists(filepath.Join(shm, "aeron-cluster")) {
		t.Fatal("match state dir should be wiped")
	}
	for _, kept := range []string{"aeron-assets/ae0/archive/0-0.rec", "aeron-emre-assets-0-driver/cnc.dat"} {
		if !exists(filepath.Join(shm, kept)) {
			t.Fatalf("match cleanup must NOT touch the assets engine's %s", kept)
		}
	}

	// Assets cleanup (full wipe) with the match dirs excluded: only its own go.
	mk("aeron-emre-0-driver/cnc.dat") // recreate a match driver dir
	if _, _, errs, _ := cleanupSweep(shm, tmp, filepath.Join(shm, "aeron-assets"), matchDirs, nil, true, true); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if exists(filepath.Join(shm, "aeron-assets")) || exists(filepath.Join(shm, "aeron-emre-assets-0-driver")) {
		t.Fatal("assets cleanup should wipe its own state + driver dirs")
	}
	if !exists(filepath.Join(shm, "aeron-emre-0-driver/cnc.dat")) {
		t.Fatal("assets cleanup must NOT touch a match driver dir")
	}
}

func TestCleanupSweepDryRunTouchesNothing(t *testing.T) {
	shm, tmp := cleanupFixture(t)

	wouldClean, _, _, _ := cleanupSweep(shm, tmp, filepath.Join(shm, "aeron-cluster"), nil, nil, false, false)
	if len(wouldClean) == 0 {
		t.Fatal("dry run should report targets")
	}
	for _, kept := range []string{
		"aeron-emre-0-driver/cnc.dat",
		"aeron-cluster/node0/archive/0-0.rec",
		"aeron-cluster/node0/cluster/cluster-mark.dat",
	} {
		if !exists(filepath.Join(shm, kept)) {
			t.Fatalf("dry run must not delete %s", kept)
		}
	}
}

func TestBackupFreshness(t *testing.T) {
	dir := t.TempDir()

	// No heartbeat file
	fresh, reason, hb := BackupFreshness(dir)
	if fresh || hb != nil || !strings.Contains(reason, "no heartbeat") {
		t.Fatalf("missing file should be stale, got fresh=%v reason=%q", fresh, reason)
	}

	// Live OK heartbeat
	writeHeartbeat(t, dir, 2, "OK")
	fresh, _, hb = BackupFreshness(dir)
	if !fresh || hb == nil || hb.LiveLogPosition != 42 {
		t.Fatalf("recent OK heartbeat should be fresh, got fresh=%v hb=%+v", fresh, hb)
	}

	// Stale heartbeat (process dead/wedged)
	writeHeartbeat(t, dir, backupHeartbeatMaxAgeSec+10, "OK")
	fresh, reason, _ = BackupFreshness(dir)
	if fresh || !strings.Contains(reason, "heartbeat stale") {
		t.Fatalf("old heartbeat should be stale, got fresh=%v reason=%q", fresh, reason)
	}

	// Recent but STALLED
	writeHeartbeat(t, dir, 2, "STALLED")
	fresh, reason, _ = BackupFreshness(dir)
	if fresh || !strings.Contains(reason, "STALLED") {
		t.Fatalf("stalled heartbeat should not be fresh, got fresh=%v reason=%q", fresh, reason)
	}
}
