// SPDX-License-Identifier: Apache-2.0
package services

import (
	"strconv"
	"sync"
	"time"
)

// Operator-visible cluster transitions, derived from the status poll.
//
// The trading UI carried this log because the match gateway is an Aeron client
// and republishes leader changes on the market feed. That path covers exactly
// one cluster: the matching engine. The Assets Engine has no such feed, so the
// money ledger's elections and node losses were visible to nobody — the same
// blind spot as its snapshot position, from the same cause (the signal existed
// only where somebody happened to already be looking).
//
// The admin gateway polls every cluster's CnC counters already. Diffing that
// observation is the one place that sees ME and AE alike, so the operator's
// record belongs here, not on the trader-facing market socket.
const (
	ClusterEventLeaderChange    = "LEADER_CHANGE"
	ClusterEventNodeUp          = "NODE_UP"
	ClusterEventNodeDown        = "NODE_DOWN"
	ClusterEventQuorumLost      = "QUORUM_LOST"
	ClusterEventQuorumRestored  = "QUORUM_RESTORED"
)

// maxRetainedClusterEvents bounds the server-side history. The trading UI kept
// this in localStorage, which made the record per-browser and lost it on a
// cache clear. Holding it here means every operator sees the same history and a
// reload does not erase it.
const maxRetainedClusterEvents = 200

// ClusterEvent is one transition in a cluster's shape.
//
// Seq is a monotonic server-side id. The backlog is replayed on every SSE
// connect, and EventSource reconnects on its own, so without a stable id a
// dropped connection would duplicate the whole history in the console.
type ClusterEvent struct {
	Seq     int64     `json:"seq"`
	Type    string    `json:"type"`
	Cluster string    `json:"cluster"`
	Display string    `json:"display"`
	NodeID  int       `json:"nodeId"`
	Detail  string    `json:"detail,omitempty"`
	At      time.Time `json:"at"`
}

// clusterObservation is the last shape seen for one cluster.
type clusterObservation struct {
	seeded  bool
	leader  int
	health  []string
	quorate bool
}

// ClusterEventLog diffs successive status observations into events, retains a
// bounded history, and fans out live to SSE subscribers.
type ClusterEventLog struct {
	mu       sync.Mutex
	observed map[string]*clusterObservation
	history  []ClusterEvent
	subs     map[int]chan ClusterEvent
	next     int
	seq      int64
}

func NewClusterEventLog() *ClusterEventLog {
	return &ClusterEventLog{
		observed: map[string]*clusterObservation{},
		subs:     map[int]chan ClusterEvent{},
	}
}

// Subscribe mirrors the process-event hub: sends are non-blocking, so a slow
// consumer loses events rather than stalling the status poll.
func (l *ClusterEventLog) Subscribe(buf int) (<-chan ClusterEvent, func()) {
	if buf <= 0 {
		buf = 64
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	id := l.next
	l.next++
	ch := make(chan ClusterEvent, buf)
	l.subs[id] = ch

	return ch, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if _, ok := l.subs[id]; ok {
			delete(l.subs, id)
			close(ch)
		}
	}
}

// Recent returns up to n of the newest events, oldest first, so a client that
// connects mid-session renders the history it missed before the live tail.
func (l *ClusterEventLog) Recent(n int) []ClusterEvent {
	l.mu.Lock()
	defer l.mu.Unlock()

	if n <= 0 || n > len(l.history) {
		n = len(l.history)
	}
	out := make([]ClusterEvent, n)
	copy(out, l.history[len(l.history)-n:])
	return out
}

// Observe records a cluster's current shape and emits an event per transition.
//
// The FIRST observation of a cluster is seeded silently: an admin restart is
// not a leader election, and replaying the whole fleet as "node up" on every
// gateway restart would train operators to ignore the feed.
func (l *ClusterEventLog) Observe(cluster, display string, leader int, health []string) {
	l.mu.Lock()

	prev, known := l.observed[cluster]
	quorate := isQuorate(health)

	if !known || !prev.seeded {
		l.observed[cluster] = &clusterObservation{
			seeded:  true,
			leader:  leader,
			health:  append([]string(nil), health...),
			quorate: quorate,
		}
		l.mu.Unlock()
		return
	}

	now := time.Now()
	var emitted []ClusterEvent
	add := func(t string, nodeID int, detail string) {
		l.seq++
		emitted = append(emitted, ClusterEvent{
			Seq: l.seq, Type: t, Cluster: cluster, Display: display,
			NodeID: nodeID, Detail: detail, At: now,
		})
	}

	// Node membership first: a leader change is usually the CONSEQUENCE of one,
	// and reading the cause before the effect is what makes a feed diagnostic.
	for i := 0; i < len(health) && i < len(prev.health); i++ {
		was, is := prev.health[i] == HealthHealthy, health[i] == HealthHealthy
		switch {
		case !was && is:
			add(ClusterEventNodeUp, i, "healthy")
		case was && !is:
			add(ClusterEventNodeDown, i, health[i])
		}
	}

	// Quorum before leader: losing quorum is the money-relevant fact, and it is
	// the one neither the trading UI nor this console has ever reported.
	if prev.quorate && !quorate {
		add(ClusterEventQuorumLost, -1, quorumDetail(health))
	} else if !prev.quorate && quorate {
		add(ClusterEventQuorumRestored, -1, quorumDetail(health))
	}

	if prev.leader != leader {
		// Losing the leader entirely is not an election result; say which it is.
		var detail string
		switch {
		case leader < 0:
			detail = "no leader (was node " + strconv.Itoa(prev.leader) + ")"
		case prev.leader < 0:
			detail = "elected node " + strconv.Itoa(leader)
		default:
			detail = "node " + strconv.Itoa(prev.leader) + " -> node " + strconv.Itoa(leader)
		}
		add(ClusterEventLeaderChange, leader, detail)
	}

	prev.leader = leader
	prev.health = append(prev.health[:0], health...)
	prev.quorate = quorate

	for _, ev := range emitted {
		l.history = append(l.history, ev)
		if len(l.history) > maxRetainedClusterEvents {
			l.history = l.history[len(l.history)-maxRetainedClusterEvents:]
		}
		for _, ch := range l.subs {
			select {
			case ch <- ev:
			default: // full: drop for this subscriber
			}
		}
	}
	l.mu.Unlock()
}

// isQuorate reports whether a majority of members are healthy. A single-node
// cluster is quorate with its one node up, which is what Raft says too.
func isQuorate(health []string) bool {
	if len(health) == 0 {
		return false
	}
	healthy := 0
	for _, h := range health {
		if h == HealthHealthy {
			healthy++
		}
	}
	return healthy > len(health)/2
}

func quorumDetail(health []string) string {
	healthy := 0
	for _, h := range health {
		if h == HealthHealthy {
			healthy++
		}
	}
	return strconv.Itoa(healthy) + "/" + strconv.Itoa(len(health)) + " healthy"
}
