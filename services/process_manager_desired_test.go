// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/openexch/admin/config"
)

// A tiny catalog with a dependency edge, no real commands needed for the
// policy tests (reconcileTargets is pure). "admin" is included to prove it is
// always excluded.
func desiredTestPM(t *testing.T, adminDir string) *ProcessManager {
	t.Helper()
	return NewProcessManagerWith(ProcessManagerOptions{
		AdminDir: adminDir,
		LogDir:   filepath.Join(t.TempDir(), "log"),
		PidDir:   filepath.Join(t.TempDir(), "pid"),
		Services: []ServiceDef{
			{Name: "driver0", Role: RoleInfra, Command: []string{"/bin/true"}},
			{Name: "node0", Role: RoleInfra, Command: []string{"/bin/true"}, DependsOn: []string{"driver0"}},
			{Name: "sim", Role: RoleInfra, Command: []string{"/bin/true"}},
			{Name: "admin", Role: RoleInfra, Command: []string{"/bin/true"}},
			{Name: "noop", Role: RoleInfra}, // no command → never managed
		},
	})
}

func names(defs []ServiceDef) map[string]bool {
	m := map[string]bool{}
	for _, d := range defs {
		m[d.Name] = true
	}
	return m
}

// Fresh box: no recorded intent → reconcile starts nothing (stays idle).
func TestReconcile_FreshStaysIdle(t *testing.T) {
	pm := desiredTestPM(t, t.TempDir())
	if got := pm.reconcileTargets(); len(got) != 0 {
		t.Fatalf("fresh box should reconcile to nothing, got %v", names(got))
	}
}

// A service the operator stopped stays stopped; running ones come back.
func TestReconcile_HonorsStopAndStart(t *testing.T) {
	pm := desiredTestPM(t, t.TempDir())
	pm.setDesired("driver0", config.DesiredRunning)
	pm.setDesired("node0", config.DesiredRunning)
	pm.setDesired("sim", config.DesiredStopped) // operator explicitly stopped the sim

	got := names(pm.reconcileTargets())
	if !got["driver0"] || !got["node0"] {
		t.Fatalf("desired-running services must be reconcile targets, got %v", got)
	}
	if got["sim"] {
		t.Fatal("a deliberately stopped service must NOT be restarted on reconcile")
	}
	if got["admin"] {
		t.Fatal("admin is self-managed and must never be a reconcile target")
	}
}

// Boot order (dependencies) is preserved: a dependency precedes its dependent.
func TestReconcile_PreservesBootOrder(t *testing.T) {
	pm := desiredTestPM(t, t.TempDir())
	pm.setDesiredAll(config.DesiredRunning)
	targets := pm.reconcileTargets()
	var iDriver, iNode int = -1, -1
	for i, d := range targets {
		switch d.Name {
		case "driver0":
			iDriver = i
		case "node0":
			iNode = i
		}
	}
	if iDriver == -1 || iNode == -1 || iDriver > iNode {
		t.Fatalf("driver0 must precede node0 in reconcile order, got %+v", targets)
	}
}

// setDesiredAll marks every managed service, never admin or command-less ones.
func TestSetDesiredAll_SkipsAdminAndNoop(t *testing.T) {
	pm := desiredTestPM(t, t.TempDir())
	pm.setDesiredAll(config.DesiredRunning)
	if pm.desiredOf("admin") != "" {
		t.Fatal("admin must never be tracked in desired state")
	}
	if pm.desiredOf("noop") != "" {
		t.Fatal("command-less services must never be tracked")
	}
	if pm.desiredOf("driver0") != config.DesiredRunning {
		t.Fatal("managed services must be tracked")
	}
}

// Intent survives a restart: a new ProcessManager over the same adminDir reads
// the persisted desired state back (a crash/disarm would not have changed it).
func TestDesiredState_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	pm := desiredTestPM(t, dir)
	pm.setDesired("driver0", config.DesiredRunning)
	pm.setDesired("sim", config.DesiredStopped)

	if _, err := os.Stat(filepath.Join(dir, "desired-state.json")); err != nil {
		t.Fatalf("desired-state.json not written: %v", err)
	}

	// New manager over the same dir (simulates an admin-gateway restart).
	pm2 := desiredTestPM(t, dir)
	if pm2.desiredOf("driver0") != config.DesiredRunning {
		t.Fatal("running intent must survive a restart")
	}
	if pm2.desiredOf("sim") != config.DesiredStopped {
		t.Fatal("stopped intent must survive a restart")
	}
}

// Blank adminDir (tests / agentd default) keeps intent in memory but writes
// no file — never panics.
func TestDesiredState_NoAdminDirNoWrite(t *testing.T) {
	pm := desiredTestPM(t, "")
	pm.setDesired("driver0", config.DesiredRunning)
	if pm.desiredOf("driver0") != config.DesiredRunning {
		t.Fatal("in-memory intent must still work without an adminDir")
	}
}

// config round-trip: only valid states survive; garbage is dropped.
func TestReadDesiredState_DropsInvalid(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "desired-state.json"),
		[]byte(`{"services":{"a":"running","b":"bogus","c":"stopped"}}`), 0644); err != nil {
		t.Fatal(err)
	}
	got := config.ReadDesiredState(dir)
	if got["a"] != config.DesiredRunning || got["c"] != config.DesiredStopped {
		t.Fatalf("valid states must load, got %v", got)
	}
	if _, ok := got["b"]; ok {
		t.Fatal("invalid state value must be dropped")
	}
}
