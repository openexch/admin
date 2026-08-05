// SPDX-License-Identifier: Apache-2.0
package services

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/logging"
)

// Compile-time conformance is asserted in process_manager_agent.go; this
// re-checks via the type system in tests too.
func TestProcessManagerImplementsProcessAgent(t *testing.T) {
	var _ agent.ProcessAgent = (*ProcessManager)(nil)
}

func TestInstallArtifact(t *testing.T) {
	pm := &ProcessManager{log: logging.Component("pm")}
	dir := t.TempDir()
	dest := filepath.Join(dir, "app.jar")
	payload := []byte("jar-bytes-v2")
	sum := sha256.Sum256(payload)

	// Happy path: content lands at dest with the requested mode.
	spec := agent.ArtifactSpec{DestPath: dest, Sha256: hex.EncodeToString(sum[:]), Mode: 0640}
	if err := pm.InstallArtifact(spec, strings.NewReader(string(payload))); err != nil {
		t.Fatalf("install: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("dest content = %q, %v", got, err)
	}
	if fi, _ := os.Stat(dest); fi.Mode().Perm() != 0640 {
		t.Fatalf("mode = %v, want 0640", fi.Mode().Perm())
	}

	// Sha mismatch: rejected, and the previous dest content stays visible.
	bad := agent.ArtifactSpec{DestPath: dest, Sha256: strings.Repeat("0", 64)}
	if err := pm.InstallArtifact(bad, strings.NewReader("evil")); err == nil {
		t.Fatal("sha mismatch must fail")
	}
	got, _ = os.ReadFile(dest)
	if string(got) != string(payload) {
		t.Fatalf("failed install must not touch dest; got %q", got)
	}

	// No partial files left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".artifact-") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestSubscribeReceivesLifecycleEvents(t *testing.T) {
	pm := &ProcessManager{log: logging.Component("pm")}
	ch, unsub := pm.Subscribe(4)
	defer unsub()

	pm.emitEvent(agent.EventCrashed, "node0", 1234, "exit 137")
	select {
	case ev := <-ch:
		if ev.Type != agent.EventCrashed || ev.Service != "node0" || ev.PID != 1234 {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("event not delivered")
	}
}

func TestSubscribeSlowConsumerNeverBlocks(t *testing.T) {
	pm := &ProcessManager{log: logging.Component("pm")}
	_, unsub := pm.Subscribe(1) // 1-slot buffer, never drained
	defer unsub()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pm.emitEvent(agent.EventStopped, "market", 0, "")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("emitEvent blocked on a full subscriber — must drop instead")
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	pm := &ProcessManager{log: logging.Component("pm")}
	ch, unsub := pm.Subscribe(1)
	unsub()
	unsub() // idempotent
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after unsubscribe")
	}
	pm.emitEvent(agent.EventStarted, "oms", 1, "") // must not panic on empty registry
}

func TestAdoptedCrashEmitsEvent(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	deadPid := cmd.Process.Pid

	dir := t.TempDir()
	proc := &managedProcess{running: true, pid: deadPid, status: "running"}
	pm := &ProcessManager{
		log:      logging.Component("pm"),
		pidDir:   dir,
		logDir:   dir,
		procs:    map[string]*managedProcess{"market": proc},
		services: []ServiceDef{{Name: "market", AutoRestart: false}},
	}
	ch, unsub := pm.Subscribe(8)
	defer unsub()

	pm.refreshAdoptedProcesses()

	select {
	case ev := <-ch:
		if ev.Type != agent.EventCrashed || ev.Service != "market" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("crash event not emitted for adopted process death")
	}
}

func TestTailLogAgent(t *testing.T) {
	dir := t.TempDir()
	pm := &ProcessManager{log: logging.Component("pm"), logDir: dir}

	if err := os.WriteFile(filepath.Join(dir, "oms.log"), []byte("a\nb\nc\nd\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// Service-name spelling aliases must resolve like the HTTP log API.
	lines, err := pm.TailLog("order-management", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0] != "c" || lines[1] != "d" {
		t.Fatalf("tail = %v", lines)
	}
	if _, err := pm.TailLog("nope", 10); err == nil {
		t.Fatal("missing log should error")
	}
	if _, err := pm.TailLog("../etc/passwd", 10); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	// A negative line count (e.g. an unvalidated wire int32) must not panic the
	// slice; it clamps to zero lines.
	for _, n := range []int{-1, 0} {
		got, err := pm.TailLog("oms", n)
		if err != nil {
			t.Fatalf("TailLog(oms, %d): %v", n, err)
		}
		if len(got) != 0 {
			t.Fatalf("TailLog(oms, %d) = %v, want empty", n, got)
		}
	}
}
