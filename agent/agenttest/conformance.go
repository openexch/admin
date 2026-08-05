// SPDX-License-Identifier: Apache-2.0

// Package agenttest is the executable specification of the ProcessAgent
// contract: every implementation — the in-process LocalAgent today, the
// remote agentd client in Horizon B — must pass RunConformance unchanged.
// Keeping the assertions here (not in services/) is what makes "the remote
// agent behaves exactly like the local one" a tested property instead of a
// design intention.
package agenttest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openexch/admin/agent"
)

// Fixture describes the catalog the factory wired into the agent under test.
type Fixture struct {
	// Simple is a manageable long-running service with AutoRestart off.
	Simple string
	// AutoRestart is a long-running service with AutoRestart on and a short
	// (~1s) restart delay — the split-brain regression needs it.
	AutoRestart string
	// LogDir holds <service>.log files on the agent's host (TailLog seeding).
	LogDir string
	// Scratch is a writable directory on the agent's filesystem (artifacts).
	Scratch string
}

// Factory returns a fresh agent under test plus its fixture. It is invoked
// once per subtest; implementations should t.Cleanup their teardown.
type Factory func(t *testing.T) (agent.ProcessAgent, Fixture)

// RunConformance drives the full ProcessAgent contract against the factory.
func RunConformance(t *testing.T, mk Factory) {
	t.Run("ListGetSummary", func(t *testing.T) {
		a, fx := mk(t)
		infos := a.List()
		if len(infos) < 2 {
			t.Fatalf("catalog should list the fixture services, got %d", len(infos))
		}
		byName := map[string]agent.ProcessInfo{}
		for _, in := range infos {
			byName[in.Name] = in
		}
		for _, name := range []string{fx.Simple, fx.AutoRestart} {
			in, ok := byName[name]
			if !ok {
				t.Fatalf("%s missing from List()", name)
			}
			if in.Running || in.Status != "stopped" {
				t.Fatalf("%s should start out stopped, got %+v", name, in)
			}
		}
		if got := a.Get(fx.Simple); got == nil || got.Name != fx.Simple {
			t.Fatalf("Get(%s) = %+v", fx.Simple, got)
		}
		if got := a.Get("no-such-service"); got != nil {
			t.Fatalf("Get(unknown) must be nil, got %+v", got)
		}
	})

	t.Run("UnknownServiceErrors", func(t *testing.T) {
		a, _ := mk(t)
		for verb, fn := range map[string]func(string) error{
			"Start": a.Start, "StartUnchecked": a.StartUnchecked,
			"Stop": a.Stop, "ForceStop": a.ForceStop, "Restart": a.Restart,
		} {
			if err := fn("no-such-service"); err == nil {
				t.Fatalf("%s(unknown) must error", verb)
			}
		}
	})

	t.Run("Lifecycle", func(t *testing.T) {
		a, fx := mk(t)
		if err := a.Start(fx.Simple); err != nil {
			t.Fatalf("start: %v", err)
		}
		in := waitFor(t, a, fx.Simple, func(in *agent.ProcessInfo) bool { return in.Running })
		if in.PID <= 0 {
			t.Fatalf("running service must report a PID, got %+v", in)
		}
		if err := a.Start(fx.Simple); err == nil {
			t.Fatal("double start must error")
		}
		if err := a.Restart(fx.Simple); err != nil {
			t.Fatalf("restart: %v", err)
		}
		in2 := waitFor(t, a, fx.Simple, func(in *agent.ProcessInfo) bool { return in.Running })
		if in2.PID == in.PID {
			t.Fatalf("restart should produce a new process, pid stayed %d", in.PID)
		}
		if err := a.ForceStop(fx.Simple); err != nil {
			t.Fatalf("force-stop: %v", err)
		}
		waitFor(t, a, fx.Simple, func(in *agent.ProcessInfo) bool { return !in.Running })
	})

	t.Run("BulkOps", func(t *testing.T) {
		a, fx := mk(t)
		results := a.StartAll()
		byService := map[string]agent.ActionResult{}
		for _, r := range results {
			byService[r.Service] = r
		}
		for _, name := range []string{fx.Simple, fx.AutoRestart} {
			r, ok := byService[name]
			if !ok || !r.Success {
				t.Fatalf("StartAll result for %s: %+v", name, r)
			}
			waitFor(t, a, name, func(in *agent.ProcessInfo) bool { return in.Running })
		}
		for _, r := range a.StopAll() {
			if !r.Success {
				t.Fatalf("StopAll failed for %s: %s", r.Service, r.Error)
			}
		}
		for _, name := range []string{fx.Simple, fx.AutoRestart} {
			waitFor(t, a, name, func(in *agent.ProcessInfo) bool { return !in.Running })
		}
	})

	t.Run("TailLog", func(t *testing.T) {
		a, fx := mk(t)
		lines := []string{"one", "two", "three", "four", "five"}
		path := filepath.Join(fx.LogDir, fx.Simple+".log")
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		got, err := a.TailLog(fx.Simple, 3)
		if err != nil {
			t.Fatalf("taillog: %v", err)
		}
		if len(got) != 3 || got[0] != "three" || got[2] != "five" {
			t.Fatalf("want last 3 lines, got %v", got)
		}
		if _, err := a.TailLog("../etc/passwd", 3); err == nil {
			t.Fatal("path traversal must be rejected")
		}
		if _, err := a.TailLog("service-without-log", 3); err == nil {
			t.Fatal("missing log file must error")
		}
	})

	t.Run("InstallArtifact", func(t *testing.T) {
		a, fx := mk(t)
		content := []byte("artifact-bytes")
		sum := sha256.Sum256(content)
		dest := filepath.Join(fx.Scratch, "artifact.jar")

		spec := agent.ArtifactSpec{DestPath: dest, Sha256: hex.EncodeToString(sum[:]), Mode: 0640}
		if err := a.InstallArtifact(spec, bytes.NewReader(content)); err != nil {
			t.Fatalf("install: %v", err)
		}
		got, err := os.ReadFile(dest)
		if err != nil || !bytes.Equal(got, content) {
			t.Fatalf("dest content wrong: %v %q", err, got)
		}
		if info, _ := os.Stat(dest); info.Mode().Perm() != 0640 {
			t.Fatalf("mode not applied: %v", info.Mode())
		}

		// Sha mismatch: dest untouched, no temp litter.
		bad := agent.ArtifactSpec{DestPath: filepath.Join(fx.Scratch, "bad.jar"), Sha256: strings.Repeat("0", 64)}
		if err := a.InstallArtifact(bad, bytes.NewReader(content)); err == nil {
			t.Fatal("sha mismatch must error")
		}
		if _, err := os.Stat(bad.DestPath); !os.IsNotExist(err) {
			t.Fatal("failed install must not create the destination")
		}
		if leftovers, _ := filepath.Glob(filepath.Join(fx.Scratch, ".artifact-*")); len(leftovers) > 0 {
			t.Fatalf("temp files left behind: %v", leftovers)
		}

		if err := a.InstallArtifact(agent.ArtifactSpec{}, bytes.NewReader(content)); err == nil {
			t.Fatal("empty dest must error")
		}
	})

	t.Run("SubscribeDeliversLifecycle", func(t *testing.T) {
		a, fx := mk(t)
		events, unsub := a.Subscribe(16)
		defer unsub()

		if err := a.Start(fx.Simple); err != nil {
			t.Fatalf("start: %v", err)
		}
		expectEvent(t, events, fx.Simple, agent.EventStarted)
		if err := a.ForceStop(fx.Simple); err != nil {
			t.Fatalf("stop: %v", err)
		}
		expectEvent(t, events, fx.Simple, agent.EventStopped)
	})

	t.Run("SlowSubscriberNeverBlocks", func(t *testing.T) {
		a, fx := mk(t)
		_, unsub := a.Subscribe(1) // deliberately tiny and never drained
		defer unsub()

		done := make(chan struct{})
		go func() {
			defer close(done)
			a.Start(fx.Simple)
			a.ForceStop(fx.Simple)
			a.Start(fx.Simple)
			a.ForceStop(fx.Simple)
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("lifecycle blocked on a slow subscriber")
		}
	})

	t.Run("UnsubscribeClosesChannel", func(t *testing.T) {
		a, _ := mk(t)
		events, unsub := a.Subscribe(4)
		unsub()
		select {
		case _, ok := <-events:
			if ok {
				t.Fatal("expected closed channel after unsubscribe")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("channel not closed after unsubscribe")
		}
		unsub() // double-unsubscribe must be safe
	})

	// The anti-split-brain property (docs/AGENT-ARCHITECTURE.md): a stop
	// through the agent is intentional and must hold — the agent's own
	// auto-restart machinery may not resurrect the service after it.
	t.Run("StopStaysStopped", func(t *testing.T) {
		a, fx := mk(t)
		if err := a.Start(fx.AutoRestart); err != nil {
			t.Fatalf("start: %v", err)
		}
		waitFor(t, a, fx.AutoRestart, func(in *agent.ProcessInfo) bool { return in.Running })
		if err := a.ForceStop(fx.AutoRestart); err != nil {
			t.Fatalf("stop: %v", err)
		}
		waitFor(t, a, fx.AutoRestart, func(in *agent.ProcessInfo) bool { return !in.Running })

		// Outwait the restart delay with margin; it must stay down.
		time.Sleep(2500 * time.Millisecond)
		if in := a.Get(fx.AutoRestart); in == nil || in.Running {
			t.Fatalf("intentionally stopped service came back: %+v", in)
		}
	})
}

// waitFor polls Get until cond holds or the deadline passes.
func waitFor(t *testing.T, a agent.ProcessAgent, name string, cond func(*agent.ProcessInfo) bool) *agent.ProcessInfo {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if in := a.Get(name); in != nil && cond(in) {
			return in
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition on %s not met within deadline; last: %+v", name, a.Get(name))
	return nil
}

// expectEvent waits for a specific (service, type) event, skipping unrelated
// ones (delivery is best-effort but ordered per subscriber).
func expectEvent(t *testing.T, events <-chan agent.Event, service string, typ agent.EventType) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("event channel closed while waiting for %s/%s", service, typ)
			}
			if ev.Service == service && ev.Type == typ {
				return
			}
		case <-deadline:
			t.Fatalf("no %s event for %s within deadline", typ, service)
		}
	}
}
