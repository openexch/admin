package services

import (
	"context"
	"log/slog"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

type reviewCascadeBarrier struct{ entered, resume chan struct{} }

func (h *reviewCascadeBarrier) Enabled(context.Context, slog.Level) bool { return true }
func (h *reviewCascadeBarrier) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *reviewCascadeBarrier) WithGroup(string) slog.Handler            { return h }
func (h *reviewCascadeBarrier) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "force-stopping dependent after crash" {
		close(h.entered)
		<-h.resume
	}
	return nil
}

func TestReviewStaleCascadePreservesRecoveredDependent(t *testing.T) {
	h := &reviewCascadeBarrier{make(chan struct{}), make(chan struct{})}
	old := exec.Command("true")
	if err := old.Start(); err != nil {
		t.Fatal(err)
	}
	dep := exec.Command("sleep", "30")
	dep.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := dep.Start(); err != nil {
		t.Fatal(err)
	}
	depDone := make(chan struct{})
	go func() { _ = dep.Wait(); close(depDone) }()
	t.Cleanup(func() { _ = dep.Process.Kill(); <-depDone })
	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Process.Kill(); _ = live.Wait() })
	proc := &managedProcess{cmd: old, pid: old.Process.Pid, running: true, status: "running", stopChan: make(chan struct{})}
	dependent := &managedProcess{cmd: dep, pid: dep.Process.Pid, running: true, status: "running", stopChan: make(chan struct{})}
	pm := &ProcessManager{log: slog.New(h), logDir: t.TempDir(), pidDir: t.TempDir(),
		procs: map[string]*managedProcess{"driver": proc, "node": dependent}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.monitor(ServiceDef{Name: "driver", AutoRestart: true, RestartCascades: []string{"node"}}, proc, old, old.Process.Pid)
	}()
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cascade not reached")
	}
	// The driver recovered while the old monitor was outside proc.mu.
	proc.mu.Lock()
	proc.cmd, proc.pid, proc.running, proc.status = live, live.Process.Pid, true, "running"
	proc.stopChan = make(chan struct{})
	proc.mu.Unlock()
	close(h.resume)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("monitor did not return")
	}
	dependent.mu.Lock()
	defer dependent.mu.Unlock()
	if !dependent.running || dependent.status != "running" || !isProcessAlive(dep.Process.Pid) {
		t.Fatalf("stale driver monitor stopped recovered driver's dependent: running=%v status=%q pid=%d", dependent.running, dependent.status, dependent.pid)
	}
}

func TestReviewNonRestartCrashPreservesReplacementStatus(t *testing.T) {
	old := exec.Command("true")
	proc := &managedProcess{cmd: old, status: "running"}
	pm := &ProcessManager{log: slog.Default(), logDir: t.TempDir()}
	// emitEvent takes this mutex after recording crashTimes and releasing proc.mu.
	pm.events.mu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); pm.handleCrash(ServiceDef{Name: "svc"}, proc, old, 0, 0, "exit", nil) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		proc.mu.Lock()
		if len(proc.crashTimes) != 0 {
			proc.cmd = exec.Command("sleep", "30")
			proc.running, proc.status = true, "running"
			proc.mu.Unlock()
			break
		}
		proc.mu.Unlock()
		if time.Now().After(deadline) {
			pm.events.mu.Unlock()
			t.Fatal("crash not recorded")
		}
		time.Sleep(time.Millisecond)
	}
	pm.events.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("crash handler did not return")
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if proc.status != "running" {
		t.Fatalf("old crash handler overwrote replacement status: %q", proc.status)
	}
}
