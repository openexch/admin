// SPDX-License-Identifier: Apache-2.0
package services

import (
	"testing"
)

func healthy(n int) []string {
	h := make([]string, n)
	for i := range h {
		h[i] = HealthHealthy
	}
	return h
}

func drain(ch <-chan ClusterEvent) []ClusterEvent {
	var out []ClusterEvent
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func typesOf(events []ClusterEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}

// An admin restart is not a fleet-wide incident. Replaying the whole cluster as
// "node up" every time the gateway starts would train an operator to ignore the
// feed, which is the failure mode that let a green canary hide a dead money path.
func TestFirstObservationIsSilent(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("assets", "Assets Engine", 0, healthy(3))

	if got := drain(ch); len(got) != 0 {
		t.Errorf("seeding emitted %v, want nothing", typesOf(got))
	}
	if got := log.Recent(0); len(got) != 0 {
		t.Errorf("seeding retained %v, want nothing", typesOf(got))
	}
}

func TestUnchangedObservationEmitsNothing(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	log.Observe("match", "Matching Engine", 0, healthy(3))
	log.Observe("match", "Matching Engine", 0, healthy(3))

	if got := drain(ch); len(got) != 0 {
		t.Errorf("steady state emitted %v, want nothing", typesOf(got))
	}
}

// The cache-miss fallback in GetStatus can call fetchStatus alongside the
// poller. A transition differ must not turn that into a duplicate event.
func TestConcurrentObserversDoNotDoubleEmit(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	log.Observe("match", "Matching Engine", 1, healthy(3))
	log.Observe("match", "Matching Engine", 1, healthy(3)) // second reader, same state

	got := drain(ch)
	if len(got) != 1 || got[0].Type != ClusterEventLeaderChange {
		t.Fatalf("events = %v, want exactly one LEADER_CHANGE", typesOf(got))
	}
}

func TestNodeDownThenUp(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("assets", "Assets Engine", 0, healthy(3))

	down := healthy(3)
	down[2] = HealthDead
	log.Observe("assets", "Assets Engine", 0, down)

	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("events = %v, want one NODE_DOWN", typesOf(got))
	}
	if got[0].Type != ClusterEventNodeDown || got[0].NodeID != 2 {
		t.Errorf("event = %+v, want NODE_DOWN for node 2", got[0])
	}
	if got[0].Cluster != "assets" || got[0].Display != "Assets Engine" {
		t.Errorf("event = %+v, want the assets cluster named", got[0])
	}
	// The detail carries WHY, not just that it went away.
	if got[0].Detail != HealthDead {
		t.Errorf("detail = %q, want the health state %q", got[0].Detail, HealthDead)
	}

	log.Observe("assets", "Assets Engine", 0, healthy(3))
	got = drain(ch)
	if len(got) != 1 || got[0].Type != ClusterEventNodeUp || got[0].NodeID != 2 {
		t.Errorf("recovery events = %v, want one NODE_UP for node 2", typesOf(got))
	}
}

// Losing quorum is the money-relevant fact and neither the trading UI nor this
// console has ever reported it. Two of three down is a stopped cluster, not two
// unrelated node losses.
func TestQuorumLostAndRestored(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("assets", "Assets Engine", 0, healthy(3))

	twoDown := healthy(3)
	twoDown[1], twoDown[2] = HealthDead, HealthDead
	log.Observe("assets", "Assets Engine", -1, twoDown)

	got := typesOf(drain(ch))
	// Cause before effect: node losses, then quorum, then the leaderless state.
	want := []string{
		ClusterEventNodeDown, ClusterEventNodeDown,
		ClusterEventQuorumLost, ClusterEventLeaderChange,
	}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}

	log.Observe("assets", "Assets Engine", 0, healthy(3))
	got = typesOf(drain(ch))
	wantBack := []string{
		ClusterEventNodeUp, ClusterEventNodeUp,
		ClusterEventQuorumRestored, ClusterEventLeaderChange,
	}
	if len(got) != len(wantBack) {
		t.Fatalf("recovery events = %v, want %v", got, wantBack)
	}
}

// One node down out of three keeps quorum. Reporting QUORUM_LOST there would be
// a false alarm, and a feed that cries wolf is a feed nobody reads.
func TestSingleNodeLossKeepsQuorum(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	oneDown := healthy(3)
	oneDown[1] = HealthDead
	log.Observe("match", "Matching Engine", 0, oneDown)

	for _, ev := range drain(ch) {
		if ev.Type == ClusterEventQuorumLost {
			t.Fatal("1/3 down reported QUORUM_LOST — 2 of 3 healthy is a majority")
		}
	}
}

func TestLeaderChangeDetailNamesBothEnds(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	log.Observe("match", "Matching Engine", 2, healthy(3))

	got := drain(ch)
	if len(got) != 1 || got[0].Detail != "node 0 -> node 2" {
		t.Fatalf("events = %+v, want one LEADER_CHANGE detailing both ends", got)
	}

	// Losing the leader entirely is a different fact from electing a new one.
	log.Observe("match", "Matching Engine", -1, healthy(3))
	got = drain(ch)
	if len(got) != 1 || got[0].Detail != "no leader (was node 2)" {
		t.Fatalf("events = %+v, want a leaderless LEADER_CHANGE", got)
	}
}

// Clusters are tracked independently: an election on the matching engine must
// not read as one on the money ledger.
func TestClustersTrackedIndependently(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)
	defer unsub()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	log.Observe("assets", "Assets Engine", 0, healthy(3))
	log.Observe("match", "Matching Engine", 1, healthy(3))

	got := drain(ch)
	if len(got) != 1 || got[0].Cluster != "match" {
		t.Fatalf("events = %+v, want one event on match only", got)
	}
}

func TestHistoryIsReplayableAndBounded(t *testing.T) {
	log := NewClusterEventLog()

	log.Observe("match", "Matching Engine", 0, healthy(3))
	for i := 0; i < maxRetainedClusterEvents+50; i++ {
		log.Observe("match", "Matching Engine", i%3, healthy(3))
	}

	all := log.Recent(0)
	if len(all) != maxRetainedClusterEvents {
		t.Errorf("history = %d events, want it capped at %d", len(all), maxRetainedClusterEvents)
	}
	if got := log.Recent(5); len(got) != 5 {
		t.Errorf("Recent(5) = %d events, want 5", len(got))
	}
	// Newest last, so an SSE replay renders in chronological order.
	if !all[len(all)-1].At.After(all[0].At) && all[len(all)-1].At != all[0].At {
		t.Error("history is not in chronological order")
	}
}

// The status poll must never stall behind a console that stopped reading.
func TestSlowSubscriberDropsRatherThanBlocks(t *testing.T) {
	log := NewClusterEventLog()
	_, unsub := log.Subscribe(1) // never drained
	defer unsub()

	done := make(chan struct{})
	go func() {
		log.Observe("match", "Matching Engine", 0, healthy(3))
		for i := 0; i < 100; i++ {
			log.Observe("match", "Matching Engine", i%3, healthy(3))
		}
		close(done)
	}()

	<-done // would deadlock if a full subscriber blocked the emit

	if len(log.Recent(0)) == 0 {
		t.Error("history lost events a subscriber could not keep up with")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	log := NewClusterEventLog()
	ch, unsub := log.Subscribe(16)

	log.Observe("match", "Matching Engine", 0, healthy(3))
	unsub()

	log.Observe("match", "Matching Engine", 1, healthy(3))

	if _, open := <-ch; open {
		t.Error("channel delivered after unsubscribe")
	}
}
