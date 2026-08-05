// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openexch/admin/logging"
)

// gateFixture builds a minimal ProcessManager with a driver0 proc and a node0
// def gated by it, without spawning NewProcessManager's pollers/adoption.
func gateFixture(t *testing.T) (*ProcessManager, *managedProcess, ServiceDef) {
	t.Helper()
	driver := &managedProcess{status: "stopped"}
	pm := &ProcessManager{
		log: logging.Component("pm"),
		procs: map[string]*managedProcess{
			"driver0": driver,
			"node0":   {status: "stopped"},
		},
	}
	cnc := filepath.Join(t.TempDir(), "cnc.dat")
	def := ServiceDef{Name: "node0", GatedBy: "driver0", WaitForFile: cnc}
	return pm, driver, def
}

func TestWaitForGateFailsFastWhenDriverDown(t *testing.T) {
	pm, driver, def := gateFixture(t)
	driver.status = "failed"
	driver.lastError = "driver init error 22 — socket buffers"

	start := time.Now()
	err := pm.waitForGate(def)
	if err == nil {
		t.Fatal("expected gate to refuse start with driver down")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("fail-fast path took %s, should not wait for the gate timeout", time.Since(start))
	}
	if !strings.Contains(err.Error(), "driver0 is not running") || !strings.Contains(err.Error(), "socket buffers") {
		t.Fatalf("error should name the driver and its lastError, got: %v", err)
	}
}

func TestWaitForGatePassesWithStableDriverAndCnc(t *testing.T) {
	pm, driver, def := gateFixture(t)
	driver.running = true
	driver.status = "running"
	driver.startedAt = time.Now().Add(-10 * time.Second)
	if err := os.WriteFile(def.WaitForFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := pm.waitForGate(def); err != nil {
		t.Fatalf("gate should pass with stable driver + cnc.dat, got: %v", err)
	}
}

func TestWaitForGateTimesOutOnUnstableDriver(t *testing.T) {
	oldTimeout, oldStable := gateTimeout, gateStableFor
	gateTimeout, gateStableFor = 600*time.Millisecond, 5*time.Second
	defer func() { gateTimeout, gateStableFor = oldTimeout, oldStable }()

	pm, driver, def := gateFixture(t)
	// Driver "running" but too young to be stable (flapping): never satisfies
	// the stability requirement within the gate timeout.
	driver.running = true
	driver.status = "running"
	driver.startedAt = time.Now()
	if err := os.WriteFile(def.WaitForFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	err := pm.waitForGate(def)
	if err == nil {
		t.Fatal("expected gate to time out on a not-yet-stable driver")
	}
	if !strings.Contains(err.Error(), "did not become stable") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForGateRequiresCncFile(t *testing.T) {
	oldTimeout := gateTimeout
	gateTimeout = 600 * time.Millisecond
	defer func() { gateTimeout = oldTimeout }()

	pm, driver, def := gateFixture(t)
	driver.running = true
	driver.status = "running"
	driver.startedAt = time.Now().Add(-10 * time.Second)
	// cnc.dat deliberately absent

	if err := pm.waitForGate(def); err == nil {
		t.Fatal("expected gate to refuse start without cnc.dat")
	}
}

func TestRearmClearsCrashWindow(t *testing.T) {
	pm, driver, _ := gateFixture(t)
	driver.crashTimes = []time.Time{time.Now(), time.Now()}
	driver.lastError = "crashed"

	pm.rearm("driver0")

	if len(driver.crashTimes) != 0 || driver.lastError != "" {
		t.Fatalf("rearm should clear crashTimes+lastError, got %d crashes, lastError=%q",
			len(driver.crashTimes), driver.lastError)
	}
}

func TestAdoptedProcessCrashDetected(t *testing.T) {
	// A dead-but-reaped PID: run a real process to completion
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	deadPid := cmd.Process.Pid

	dir := t.TempDir()
	proc := &managedProcess{running: true, pid: deadPid, status: "running"} // adopted: cmd == nil
	pm := &ProcessManager{
		log:    logging.Component("pm"),
		pidDir: dir,
		logDir: dir,
		procs:  map[string]*managedProcess{"market": proc},
		services: []ServiceDef{
			{Name: "market", AutoRestart: false}, // no restart: end state is "crashed"
		},
	}

	pm.refreshAdoptedProcesses()

	// handleCrash runs on a goroutine; wait for the verdict
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		proc.mu.Lock()
		status, lastErr, running := proc.status, proc.lastError, proc.running
		proc.mu.Unlock()
		if status == "crashed" {
			if running {
				t.Fatal("crashed process must not be marked running")
			}
			if !strings.Contains(lastErr, "adopted process died") {
				t.Fatalf("lastError should carry the adopted-crash cause, got %q", lastErr)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	t.Fatalf("adopted crash not detected: status=%q running=%v", proc.status, proc.running)
}

func TestTailLogSnippet(t *testing.T) {
	dir := t.TempDir()

	// Missing file → empty
	if got := tailLogSnippet(filepath.Join(dir, "missing.log")); got != "" {
		t.Fatalf("missing file should yield empty snippet, got %q", got)
	}

	// Last 3 non-empty lines, joined
	logPath := filepath.Join(dir, "svc.log")
	content := "line1\nline2\n\nline3\nline4\nline5\n\n"
	if err := os.WriteFile(logPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	got := tailLogSnippet(logPath)
	if got != "line3 | line4 | line5" {
		t.Fatalf("unexpected snippet: %q", got)
	}

	// Long content truncates from the front
	long := strings.Repeat("a", 500)
	if err := os.WriteFile(logPath, []byte(long), 0644); err != nil {
		t.Fatal(err)
	}
	got = tailLogSnippet(logPath)
	if len(got) > 310 || !strings.HasPrefix(got, "…") {
		t.Fatalf("long snippet should truncate with leading ellipsis, got len=%d", len(got))
	}
}
