// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/openexch/admin/logging"
)

// Regression: a monitor goroutine belongs to ONE process incarnation.
//
// Production interleaving: incarnation N of a service is stopped (SIGTERM,
// state cleared, stopChan closed by stopProcess) and immediately started
// again. The new start completes — proc now tracks incarnation N+1: a LIVE
// process, a new pid, a fresh open stopChan, a new logFile, a fresh pid file —
// while the monitor spawned for incarnation N is still between cmd.Wait()
// returning and acquiring proc.mu. When that stale monitor finally runs it
// must not touch the new incarnation's state: marking a live process
// not-running, zeroing its pid, closing its log file, deleting its pid file
// and running the crash protocol (a phantom crash, burning the #17 crash-loop
// budget — and with AutoRestart a duplicate start of an already-live process).
func TestMonitorStaleIncarnationDoesNotClobberLiveState(t *testing.T) {
	logDir := t.TempDir()
	pidDir := t.TempDir()
	pm := &ProcessManager{
		log:    logging.Component("pm"),
		logDir: logDir,
		pidDir: pidDir,
		procs:  map[string]*managedProcess{"svc": {status: "stopped"}},
	}
	proc := pm.procs["svc"]

	// The stale incarnation's handle: a process that has already exited, so
	// the monitor's Wait() returns immediately. The monitor owns that Wait
	// call — do not wait on it here. NOTE: we must NOT spin on
	// isProcessAlive(stale.Pid) to confirm exit: an exited-but-unreaped child
	// is a zombie, and signal-0 on a zombie still reports "alive" until someone
	// calls Wait() (the monitor does). A short sleep lets `true` exit.
	stale := exec.Command("true")
	if err := stale.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	staleStop := make(chan struct{})
	close(staleStop) // the old incarnation's stopChan, closed by its stopProcess

	// The current live incarnation the tracked state belongs to.
	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		live.Process.Kill()
		live.Wait()
	})

	logFile, err := os.Create(filepath.Join(logDir, "svc.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logFile.Close() })

	pm.writePID("svc", live.Process.Pid)

	proc.mu.Lock()
	proc.cmd = stale // what the stale monitor captured at spawn (incarnation N)
	proc.pid = live.Process.Pid
	proc.running = true
	proc.status = "running"
	proc.stopChan = make(chan struct{}) // incarnation N+1's open channel
	proc.logFile = logFile
	proc.mu.Unlock()

	def := ServiceDef{Name: "svc", AutoRestart: false}
	pm.monitor(def, proc)

	proc.mu.Lock()
	defer proc.mu.Unlock()
	if !proc.running {
		t.Error("stale monitor marked the live incarnation not-running")
	}
	if proc.pid != live.Process.Pid {
		t.Errorf("stale monitor clobbered pid: got %d, want %d", proc.pid, live.Process.Pid)
	}
	if proc.status != "running" {
		t.Errorf("stale monitor changed status: got %q, want %q", proc.status, "running")
	}
	if len(proc.crashTimes) != 0 {
		t.Errorf("stale monitor recorded a phantom crash: %v", proc.crashTimes)
	}
	if proc.logFile == nil {
		t.Error("stale monitor closed the live incarnation's log file")
	} else if _, err := proc.logFile.WriteString("still-open\n"); err != nil {
		t.Errorf("live incarnation's log file is broken: %v", err)
	}
	if got := pm.readPID("svc"); got != live.Process.Pid {
		t.Errorf("stale monitor removed the live pid file: readPID = %d, want %d", got, live.Process.Pid)
	}
}
