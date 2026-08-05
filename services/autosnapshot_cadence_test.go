// SPDX-License-Identifier: Apache-2.0
package services

import (
	"strings"
	"testing"
	"time"
)

// A cadence test needs a target whose log size and clock it can drive, without
// a cluster behind it.
func cadenceTarget(t *testing.T, name string, logBytes int64) (*AutoSnapshot, snapshotTarget) {
	t.Helper()
	ops := &OperationsService{cluster: &Cluster{Name: name}, progress: NewProgress()}
	ops.statusSvc = &StatusService{
		cachedStatus: map[string]interface{}{
			"clusters": []map[string]interface{}{
				{
					"name":            name,
					"allNodesHealthy": true,
					"nodes": []map[string]interface{}{
						{"id": 0, "logDelta": logBytes},
					},
				},
			},
		},
		lastUpdate: time.Now(),
	}

	a := NewAutoSnapshot(map[string]*OperationsService{name: ops})
	a.intervalMinutes = 5
	return a, a.targets[0]
}

// The byte trigger is what holds the ledger-stays-in-RAM decision: at 400k/s a
// five-minute timer would let ~13.5 GB of log accumulate against a 7.8 GB
// tmpfs. It must fire well before the timer would.
func TestByteThresholdFiresBeforeTheTimer(t *testing.T) {
	a, target := cadenceTarget(t, "assets", defaultSnapshotByteThreshold+1)
	a.clocks["assets"] = &cadenceClock{since: time.Now()} // timer nowhere near due

	reason := a.dueReason(target)
	if reason == "" {
		t.Fatal("a log past the byte threshold did not trigger a snapshot")
	}
	if !strings.Contains(reason, "log grew") {
		t.Errorf("reason = %q, want it to name the log size, not the timer", reason)
	}
}

func TestBelowThresholdAndWithinIntervalDoesNotFire(t *testing.T) {
	a, target := cadenceTarget(t, "assets", 1024)
	a.clocks["assets"] = &cadenceClock{since: time.Now()}

	if reason := a.dueReason(target); reason != "" {
		t.Errorf("a quiet, recently snapshotted cluster fired: %q", reason)
	}
}

// The timer is the floor for quiet markets: a slow day must still produce
// recent restore points, or a demo box that trades nothing has nothing to
// restore from.
func TestTimerStillFiresOnAQuietCluster(t *testing.T) {
	a, target := cadenceTarget(t, "assets", 1024)
	a.clocks["assets"] = &cadenceClock{since: time.Now().Add(-10 * time.Minute)}

	reason := a.dueReason(target)
	if reason == "" {
		t.Fatal("a cluster idle past its interval did not snapshot")
	}
	if !strings.Contains(reason, "interval") {
		t.Errorf("reason = %q, want it to name the interval", reason)
	}
}

// An unreadable log size must degrade to the old fixed cadence, never to
// silence. Treating "unknown" as zero would mean a cluster whose status broke
// simply stops snapshotting, which is the 2026-07-25 failure with a new cause.
func TestUnknownLogSizeFallsBackToTheTimer(t *testing.T) {
	ops := &OperationsService{cluster: &Cluster{Name: "assets"}, progress: NewProgress()}
	// No status service at all: LogBytesSinceSnapshot returns -1.
	a := NewAutoSnapshot(map[string]*OperationsService{"assets": ops})
	a.intervalMinutes = 5
	target := a.targets[0]

	if got := ops.LogBytesSinceSnapshot(); got != -1 {
		t.Fatalf("LogBytesSinceSnapshot = %d, want -1 for an unreadable status", got)
	}

	a.clocks["assets"] = &cadenceClock{since: time.Now()}
	if reason := a.dueReason(target); reason != "" {
		t.Errorf("unknown log size fired early: %q", reason)
	}

	a.clocks["assets"] = &cadenceClock{since: time.Now().Add(-10 * time.Minute)}
	if reason := a.dueReason(target); reason == "" {
		t.Error("unknown log size suppressed the timer — a broken status must not stop snapshots")
	}
}

// A gateway restart must not fire a snapshot on every cluster at once. Start
// seeds the clock; a genuinely busy cluster still fires immediately, on bytes.
func TestStartSeedsTheClockSoARestartDoesNotStorm(t *testing.T) {
	a, target := cadenceTarget(t, "assets", 1024)

	a.Start(5)
	defer a.Stop()

	if reason := a.dueReason(target); reason != "" {
		t.Errorf("a freshly started scheduler fired immediately: %q", reason)
	}
}

// The status has to say WHY, or an operator cannot tell a cluster snapshotting
// on volume from one merely ticking over — which is the signal that throughput
// has outgrown the timer.
func TestStatusReportsTriggerAndBacklog(t *testing.T) {
	a, target := cadenceTarget(t, "assets", 4096)
	a.noteTrigger(target.name, "log grew 5 bytes since the last snapshot (threshold 1)")

	m := a.ToMap()
	if m["byteThreshold"] != defaultSnapshotByteThreshold {
		t.Errorf("byteThreshold = %v, want it reported", m["byteThreshold"])
	}

	clusters := m["clusters"].(map[string]interface{})
	assets := clusters["assets"].(map[string]interface{})
	if assets["logBytesSinceSnapshot"] != int64(4096) {
		t.Errorf("logBytesSinceSnapshot = %v, want 4096", assets["logBytesSinceSnapshot"])
	}
	if trigger, _ := assets["lastTrigger"].(string); !strings.Contains(trigger, "log grew") {
		t.Errorf("lastTrigger = %v, want the reason surfaced", assets["lastTrigger"])
	}
}

// Seeding the cadence clock must NOT write a success. The two were briefly the
// same field and the status immediately reported "snapshotted 2 minutes ago"
// for a cluster that had never snapshotted — a restore point that did not
// exist, which is the exact signal this system must never invent.
func TestSeedingTheClockDoesNotInventASuccess(t *testing.T) {
	a, _ := cadenceTarget(t, "assets", 1024)

	a.Start(5)
	defer a.Stop()

	clusters := a.ToMap()["clusters"].(map[string]interface{})
	assets := clusters["assets"].(map[string]interface{})

	if _, claimed := assets["lastSuccess"]; claimed {
		t.Error("a freshly started scheduler claimed a snapshot it never took")
	}
	if assets["snapshots"] != 0 {
		t.Errorf("snapshots = %v, want 0", assets["snapshots"])
	}
}
