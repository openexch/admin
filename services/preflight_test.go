// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
)

const meminfoSample = `MemTotal:       32618172 kB
MemFree:         1479060 kB
MemAvailable:    7935928 kB
Buffers:          361804 kB
`

func TestMemAvailableBytes(t *testing.T) {
	got, err := memAvailableBytes([]byte(meminfoSample))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := int64(7935928) * 1024; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	if _, err := memAvailableBytes([]byte("MemTotal: 1 kB\n")); err == nil {
		t.Fatal("expected error when MemAvailable is absent")
	}
	if _, err := memAvailableBytes([]byte("MemAvailable: garbage kB\n")); err == nil {
		t.Fatal("expected error on unparseable value")
	}
}

// writeMeminfo writes a meminfo fixture reporting availMB and returns its path.
func writeMeminfo(t *testing.T, availMB int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meminfo")
	content := "MemTotal: 32618172 kB\nMemAvailable: " + strconv.FormatInt(availMB*1024, 10) + " kB\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func newTestPreflight(cfg *config.Config) *Preflight {
	p := NewPreflight(cfg)
	p.driverDir = func(nodeId int) string { return "/nonexistent/test-driver" } // never the real /dev/shm
	return p
}

func TestCheckMemAvailableTiers(t *testing.T) {
	cfg := &config.Config{MinMemMB: 4096}
	cases := []struct {
		availMB  int64
		ok       bool
		severity string
	}{
		{8192, true, ""},            // above warn (6144)
		{5000, false, SeverityWarn}, // between block and warn
		{3000, false, SeverityBlock},
	}
	for _, c := range cases {
		p := newTestPreflight(cfg)
		p.meminfoPath = writeMeminfo(t, c.availMB)
		r := p.checkMemAvailable()
		if r.OK != c.ok || r.Severity != c.severity {
			t.Errorf("availMB=%d: got ok=%v severity=%q detail=%q, want ok=%v severity=%q",
				c.availMB, r.OK, r.Severity, r.Detail, c.ok, c.severity)
		}
		if !strings.Contains(r.Detail, "ADMIN_MIN_MEM_MB") {
			t.Errorf("detail should name the knob, got %q", r.Detail)
		}
	}
}

func TestCheckMemAvailableUnreadable(t *testing.T) {
	p := newTestPreflight(&config.Config{MinMemMB: 4096})
	p.meminfoPath = "/nonexistent/meminfo"
	r := p.checkMemAvailable()
	if r.OK || r.Severity != SeverityWarn {
		t.Fatalf("unreadable meminfo should warn, got %+v", r)
	}
}

func TestCheckArtifacts(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "present.jar")
	if err := os.WriteFile(present, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.jar")

	cfg := &config.Config{JarPath: present, GatewayJar: present, OmsJar: present, SimBinary: present, AssetsJar: present, AssetsBridgeJar: present}
	p := newTestPreflight(cfg)
	if r := p.checkArtifacts(); !r.OK {
		t.Fatalf("all present should pass, got %+v", r)
	}

	cfg.OmsJar = missing
	r := p.checkArtifacts()
	if r.OK || r.Severity != SeverityWarn {
		t.Fatalf("missing artifact should warn, got %+v", r)
	}
	if !strings.Contains(r.Detail, "missing.jar") || !strings.Contains(r.Detail, "oms") {
		t.Fatalf("detail should name path and dependents, got %q", r.Detail)
	}
}

// TestIsTmpfsMagic exercises the pure magic-number comparison directly, with
// no filesystem/statfs involved at all, so it can never be flaky regardless
// of what the build/test sandbox's /tmp or /dev/shm happen to be backed by.
func TestIsTmpfsMagic(t *testing.T) {
	if !isTmpfsMagic(0x01021994) {
		t.Fatal("TMPFS_MAGIC must report tmpfs")
	}
	cases := []int64{
		0xEF53,     // ext4
		0x58465342, // xfs
		0,
		-1,
	}
	for _, magic := range cases {
		if isTmpfsMagic(magic) {
			t.Fatalf("magic %#x must not report tmpfs", magic)
		}
	}
}

// TestCheckAssetsStateOnDisk_NotTmpfs uses t.TempDir(), which on this box (and
// most CI/dev boxes: no TMPDIR override) resolves under the OS temp dir on a
// real disk filesystem, not tmpfs. Confirmed for this environment via `stat -f`
// before writing this test. Guarded regardless: if the temp dir turns out to
// be tmpfs-backed here, the test skips rather than asserting a false failure.
func TestCheckAssetsStateOnDisk_NotTmpfs(t *testing.T) {
	dir := t.TempDir()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err == nil && isTmpfsMagic(int64(st.Type)) {
		t.Skip("t.TempDir() is tmpfs-backed in this environment; the pure isTmpfsMagic test covers the comparison logic")
	}

	p := newTestPreflight(&config.Config{AssetsStateDir: dir})
	r := p.checkAssetsStateOnDisk()
	if !r.OK {
		t.Fatalf("a non-tmpfs dir must not trigger the finding, got %+v", r)
	}
}

// TestCheckAssetsStateOnDisk_Tmpfs uses /dev/shm directly, which exists and is
// tmpfs on Linux dev/CI boxes; skips gracefully if it is absent or turns out
// not to be tmpfs in some sandbox.
func TestCheckAssetsStateOnDisk_Tmpfs(t *testing.T) {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/dev/shm", &st); err != nil {
		t.Skip("/dev/shm not present in this sandbox")
	}
	if !isTmpfsMagic(int64(st.Type)) {
		t.Skip("/dev/shm is not tmpfs-backed in this sandbox")
	}

	t.Run("default is warn", func(t *testing.T) {
		p := newTestPreflight(&config.Config{AssetsStateDir: "/dev/shm"})
		r := p.checkAssetsStateOnDisk()
		if r.OK || r.Severity != SeverityWarn {
			t.Fatalf("tmpfs must warn by default, got %+v", r)
		}
		if !strings.Contains(r.Detail, "ASSETS_STATE_DIR") {
			t.Fatalf("detail should name the fix knob, got %q", r.Detail)
		}
		if strings.Contains(r.Detail, "—") {
			t.Fatalf("detail must not contain an em-dash, got %q", r.Detail)
		}
	})

	t.Run("escalates to block under ASSETS_REQUIRE_DISK", func(t *testing.T) {
		t.Setenv("ASSETS_REQUIRE_DISK", "true")
		p := newTestPreflight(&config.Config{AssetsStateDir: "/dev/shm"})
		r := p.checkAssetsStateOnDisk()
		if r.OK || r.Severity != SeverityBlock {
			t.Fatalf("tmpfs with ASSETS_REQUIRE_DISK=true must block, got %+v", r)
		}
	})
}

func TestCheckAssetsStateOnDisk_UnstatableDir(t *testing.T) {
	p := newTestPreflight(&config.Config{AssetsStateDir: "/nonexistent/assets-state-dir"})
	r := p.checkAssetsStateOnDisk()
	if r.OK || r.Severity != SeverityWarn {
		t.Fatalf("an unstatable dir should warn (never silently pass, never block), got %+v", r)
	}
}

func TestCheckAssetsStateFreeSpace(t *testing.T) {
	dir := t.TempDir()
	p := newTestPreflight(&config.Config{AssetsStateDir: dir})
	r := p.checkAssetsStateFreeSpace()
	// This box's temp filesystem is assumed to have >5GB free; assert the
	// invariant this check must always uphold (never blocking) rather than the
	// numeric threshold, which would be flaky across environments.
	if r.Severity == SeverityBlock {
		t.Fatalf("ae-state-free-space must never block, got %+v", r)
	}

	unstatable := newTestPreflight(&config.Config{AssetsStateDir: "/nonexistent/assets-state-dir"})
	r2 := unstatable.checkAssetsStateFreeSpace()
	if r2.OK || r2.Severity != SeverityWarn {
		t.Fatalf("an unstatable dir should warn, got %+v", r2)
	}
}

func TestCheckDriverDirs(t *testing.T) {
	fa := newFakeAgent()
	p := newTestPreflight(&config.Config{})
	p.SetProcessManager(fa)

	dir := t.TempDir()
	p.driverDir = func(nodeId int) string { return dir }

	// Not running: no failure regardless of dir state.
	fa.set(agent.ProcessInfo{Name: "driver0", Running: false})
	if r := p.checkDriverDirs(); !r.OK {
		t.Fatalf("stopped driver should not fail, got %+v", r)
	}

	// Running with our own (alive) PID but no cnc.dat: the #42 invariant fires.
	fa.set(agent.ProcessInfo{Name: "driver0", Running: true, PID: os.Getpid()})
	r := p.checkDriverDirs()
	if r.OK || r.Severity != SeverityBlock {
		t.Fatalf("alive driver with missing cnc.dat must block, got %+v", r)
	}
	if !strings.Contains(r.Detail, "driver0") || !strings.Contains(r.Detail, "runbook 1") {
		t.Fatalf("detail should identify the driver and remediation, got %q", r.Detail)
	}

	// cnc.dat present: healthy.
	if err := os.WriteFile(filepath.Join(dir, "cnc.dat"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if r := p.checkDriverDirs(); !r.OK {
		t.Fatalf("driver with cnc.dat should pass, got %+v", r)
	}
}

// fakeStatus feeds the quorum check a scripted status map.
type fakeStatus struct{ status map[string]interface{} }

func (f *fakeStatus) GetStatus() map[string]interface{} { return f.status }

func statusWithHealth(health ...string) map[string]interface{} {
	nodes := make([]map[string]interface{}, len(health))
	for i, h := range health {
		nodes[i] = map[string]interface{}{"id": i, "health": h}
	}
	return map[string]interface{}{"nodes": nodes}
}

func TestCheckQuorum(t *testing.T) {
	p := newTestPreflight(&config.Config{})

	p.status = &fakeStatus{statusWithHealth(HealthHealthy, HealthHealthy, HealthHealthy)}
	if r := p.checkQuorum(); !r.OK {
		t.Fatalf("3/3 healthy should pass, got %+v", r)
	}

	p.status = &fakeStatus{statusWithHealth(HealthHealthy, HealthDead, HealthOffline)}
	r := p.checkQuorum()
	if r.OK || r.Severity != SeverityBlock {
		t.Fatalf("1/3 healthy must block, got %+v", r)
	}
	if !strings.Contains(r.Detail, "1/3 healthy") || !strings.Contains(r.Detail, "node1=DEAD") {
		t.Fatalf("detail should carry per-node states, got %q", r.Detail)
	}
}

func TestGate(t *testing.T) {
	// Disk thresholds sized so the real statfs values never trip in a test env.
	cfg := &config.Config{MinMemMB: 4096, MinRootDiskGB: 0, MaxShmUsedPct: 100}

	// Unknown op: never gated.
	p := newTestPreflight(cfg)
	p.meminfoPath = writeMeminfo(t, 1000)
	if err := p.Gate("snapshot", false); err != nil {
		t.Fatalf("ungated op must pass, got %v", err)
	}

	// Blocking mem failure refuses rolling-update...
	err := p.Gate("rolling-update", false)
	if err == nil {
		t.Fatal("expected refusal on low memory")
	}
	if !strings.Contains(err.Error(), "mem-available") || !strings.Contains(err.Error(), "force") {
		t.Fatalf("refusal should name the check and the override, got %v", err)
	}

	// ...and force overrides it.
	if err := p.Gate("rolling-update", true); err != nil {
		t.Fatalf("force must override, got %v", err)
	}

	// Warn-tier failure alone does not refuse.
	p.meminfoPath = writeMeminfo(t, 5000)
	if err := p.Gate("rolling-update", false); err != nil {
		t.Fatalf("warn-only failure must not refuse, got %v", err)
	}

	// Quorum loss refuses even with memory fine.
	p.meminfoPath = writeMeminfo(t, 8192)
	p.status = &fakeStatus{statusWithHealth(HealthHealthy, HealthHealthy, HealthDegraded)}
	err = p.Gate("rolling-update", false)
	if err == nil || !strings.Contains(err.Error(), "cluster-quorum") {
		t.Fatalf("expected quorum refusal, got %v", err)
	}
}

// GateForCluster scopes the match-specific checks (cluster-quorum, driver-dirs)
// out for any non-match cluster, while the box-global checks (mem, disk) still
// apply (ag#83). A degraded matching engine must not refuse an assets op.
func TestGateForClusterScoping(t *testing.T) {
	cfg := &config.Config{MinMemMB: 4096, MinRootDiskGB: 0, MaxShmUsedPct: 100}
	p := newTestPreflight(cfg)
	p.meminfoPath = writeMeminfo(t, 8192) // mem fine
	p.status = &fakeStatus{statusWithHealth(HealthHealthy, HealthHealthy, HealthDead)}

	// Matching engine: the quorum check applies and refuses.
	if err := p.GateForCluster("rolling-update", "match", false); err == nil || !strings.Contains(err.Error(), "cluster-quorum") {
		t.Fatalf("match op must be gated by quorum, got %v", err)
	}
	// Gate() is the match-scoped alias, so it refuses identically.
	if err := p.Gate("rolling-update", false); err == nil || !strings.Contains(err.Error(), "cluster-quorum") {
		t.Fatalf("Gate must behave as the match-scoped GateForCluster, got %v", err)
	}
	// Assets: quorum + driver-dirs scoped out, so the degraded ME does not gate it.
	if err := p.GateForCluster("rolling-update", "assets", false); err != nil {
		t.Fatalf("assets op must not be gated by the match-specific quorum check, got %v", err)
	}
	// The global mem check still gates the assets op.
	p.meminfoPath = writeMeminfo(t, 1000)
	if err := p.GateForCluster("rolling-update", "assets", false); err == nil || !strings.Contains(err.Error(), "mem-available") {
		t.Fatalf("assets op must still be gated by the global mem check, got %v", err)
	}
}

func TestInvariantsOK(t *testing.T) {
	if !InvariantsOK([]InvariantResult{{OK: true}, {OK: true}}) {
		t.Fatal("all ok should be true")
	}
	if InvariantsOK([]InvariantResult{{OK: true}, {OK: false}}) {
		t.Fatal("any failure should be false")
	}
}
