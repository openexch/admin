// SPDX-License-Identifier: Apache-2.0
package services

import (
	"testing"
	"time"
)

// The regression that cost the money path on 2026-07-25: the scheduler was
// constructed with ONE OperationsService (the matching engine's), so the Assets
// Engine was never auto-snapshotted, its cluster log grew without bound, tmpfs
// filled, and the AE nodes started dying on "No space left on device".
//
// Every cluster the HTTP layer can route to must be visible to the scheduler.
func TestAutoSnapshotCoversEveryCluster(t *testing.T) {
	as := NewAutoSnapshot(map[string]*OperationsService{
		"match":  {},
		"assets": {},
	})

	clusters := as.ToMap()["clusters"].(map[string]interface{})
	for _, name := range []string{"match", "assets"} {
		if _, ok := clusters[name]; !ok {
			t.Fatalf("cluster %q is not scheduled for auto-snapshot; its log will grow unbounded", name)
		}
	}
	if len(as.targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(as.targets))
	}
}

// Cycle order must not depend on Go's map iteration, or which cluster gets
// snapshotted first (and which one loses the shared operation slot) changes
// run to run.
func TestAutoSnapshotCycleOrderIsDeterministic(t *testing.T) {
	for i := 0; i < 20; i++ {
		as := NewAutoSnapshot(map[string]*OperationsService{
			"match": {}, "assets": {}, "zebra": {},
		})
		got := []string{as.targets[0].name, as.targets[1].name, as.targets[2].name}
		want := []string{"assets", "match", "zebra"}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("cycle order = %v, want %v", got, want)
			}
		}
	}
}

// A refused snapshot must be counted, not just logged: a cluster that refuses
// every cycle looks identical to one that is snapshotting fine unless the
// outcome is recorded and surfaced.
func TestAutoSnapshotRecordsOutcomesPerCluster(t *testing.T) {
	as := NewAutoSnapshot(map[string]*OperationsService{"match": {}, "assets": {}})

	as.recordFailure("match", "refusing snapshot: node2=OFFLINE")
	as.recordFailure("match", "refusing snapshot: node2=OFFLINE")
	as.recordSuccess("assets")

	if got := as.consecutiveFails("match"); got != 2 {
		t.Fatalf("match consecutive failures = %d, want 2", got)
	}
	// One cluster failing must never be recorded against the other.
	if got := as.consecutiveFails("assets"); got != 0 {
		t.Fatalf("assets consecutive failures = %d, want 0", got)
	}

	clusters := as.ToMap()["clusters"].(map[string]interface{})
	match := clusters["match"].(map[string]interface{})
	if match["consecutiveFailures"].(int) != 2 {
		t.Fatalf("match consecutiveFailures not surfaced: %v", match)
	}
	if match["lastError"] != "refusing snapshot: node2=OFFLINE" {
		t.Fatalf("match lastError not surfaced: %v", match)
	}
	if _, reported := match["lastSuccess"]; reported {
		t.Fatal("match reported a lastSuccess it never had")
	}

	assets := clusters["assets"].(map[string]interface{})
	if assets["snapshots"].(int) != 1 {
		t.Fatalf("assets snapshots = %v, want 1", assets["snapshots"])
	}
	if _, reported := assets["lastSuccess"]; !reported {
		t.Fatal("assets success not surfaced")
	}
}

// A success clears the failure streak so a recovered cluster stops alarming.
func TestAutoSnapshotSuccessClearsFailureStreak(t *testing.T) {
	as := NewAutoSnapshot(map[string]*OperationsService{"assets": {}})
	as.recordFailure("assets", "another operation in progress")
	as.recordSuccess("assets")

	if got := as.consecutiveFails("assets"); got != 0 {
		t.Fatalf("consecutive failures = %d after success, want 0", got)
	}
	entry := as.ToMap()["clusters"].(map[string]interface{})["assets"].(map[string]interface{})
	if entry["lastError"] != nil {
		t.Fatalf("stale lastError still surfaced: %v", entry["lastError"])
	}
	if secs, ok := entry["secondsSinceLastSuccess"].(int64); !ok || secs > 5 {
		t.Fatalf("secondsSinceLastSuccess = %v, want a fresh value", entry["secondsSinceLastSuccess"])
	}
}

// Stop must not panic or leak when called twice, or when never started.
func TestAutoSnapshotStopIsIdempotent(t *testing.T) {
	as := NewAutoSnapshot(map[string]*OperationsService{"assets": {}})
	as.Stop()
	as.Start(1)
	if !as.IsEnabled() {
		t.Fatal("not enabled after Start")
	}
	as.Stop()
	as.Stop()
	if as.IsEnabled() {
		t.Fatal("still enabled after Stop")
	}
	time.Sleep(10 * time.Millisecond)
}
