// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"sync"
	"time"
)

// How far the log can be reclaimed: the lowest position every consumer has
// already passed.
//
// This replaces a boolean. Housekeeping used to ask "are all members healthy?"
// and then either purge everything below the snapshot or purge nothing. Both
// answers are wrong. One strands a member that was offline at snapshot time;
// the other grows without bound, and that branch is what left the Assets Engine
// with an unbounded log, a full /dev/shm and a dead money path for seventeen
// hours while every health check stayed green.
//
// A watermark has neither failure mode. It becomes conservative on its own when
// a member falls behind and aggressive again the moment everyone catches up,
// and it is provably safe at every point in between because it never passes a
// consumer that still needs the data.
type Watermark struct {
	// Position is what to purge below. Callers pass it to the housekeeping tool.
	Position int64
	// Limiter names the consumer holding it down, for the operator who wants to
	// know why the disk is not shrinking.
	Limiter string
	// Detail is every input, so a surprising watermark can be read rather than
	// reverse-engineered.
	Detail map[string]int64
	// Stranded members have held the watermark past the bound and been given
	// up on; they must be reseeded.
	Stranded []int
}

// laggard tracks how long one member has been holding the watermark down.
type laggard struct {
	since    time.Time
	position int64
}

// WatermarkTracker computes the retention watermark and enforces the bound on a
// member that will not catch up.
//
// The bound is required, not optional. Without it a single member that is far
// behind holds the watermark down indefinitely, the log grows, and the disk
// fills anyway — the outage returns through the safety mechanism meant to
// prevent it. Bounding it converts an accidental disk-full into an explicit,
// visible decision: this member is stranded, stop waiting for it, reseed it.
type WatermarkTracker struct {
	mu sync.Mutex
	// laggards is keyed cluster/node.
	laggards map[string]*laggard
	stranded map[string]bool

	// maxLagBytes and maxLagDuration bound how long a behind member holds the
	// watermark. Opening values from the design: 2 GB or 30 minutes, whichever
	// comes first.
	maxLagBytes    int64
	maxLagDuration time.Duration
}

const (
	defaultMaxLagBytes    int64 = 2 << 30 // 2 GiB
	defaultMaxLagDuration       = 30 * time.Minute
)

func NewWatermarkTracker() *WatermarkTracker {
	return &WatermarkTracker{
		laggards:       map[string]*laggard{},
		stranded:       map[string]bool{},
		maxLagBytes:    defaultMaxLagBytes,
		maxLagDuration: defaultMaxLagDuration,
	}
}

// MemberPosition is one member's replicated position and whether we can trust it.
//
// Trust matters: Aeron's CnC counters survive the process that wrote them, so a
// dead node reports a stale position that looks alive. Treating that as current
// would let the watermark advance past a member that has not actually received
// anything, and the purge would strand it for real.
type MemberPosition struct {
	NodeID   int
	Position int64
	Trusted  bool
}

// Compute returns the watermark for one cluster.
//
// snapshotPosition is the ceiling: recovery on each node needs everything above
// its newest snapshot, so nothing above it is ever reclaimable regardless of
// what the other inputs say.
//
// s3Verified and backupPosition are -1 when that consumer does not exist or has
// stored nothing. A -1 does NOT hold the watermark at -1: a consumer that has
// never stored anything is not a consumer waiting for data, it is one that is
// not participating. Treating it as a constraint would mean a box with no
// bucket configured could never reclaim anything.
func (t *WatermarkTracker) Compute(cluster string, snapshotPosition int64,
	members []MemberPosition, s3Verified, backupPosition int64) Watermark {

	w := Watermark{
		Position: snapshotPosition,
		Limiter:  "snapshot",
		Detail: map[string]int64{
			"snapshot": snapshotPosition,
		},
	}

	consider := func(name string, pos int64) {
		if pos < 0 {
			return // not participating
		}
		w.Detail[name] = pos
		if pos < w.Position {
			w.Position = pos
			w.Limiter = name
		}
	}

	consider("s3Verified", s3Verified)
	consider("backup", backupPosition)

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	var slowest int64 = -1
	slowestNode := -1

	for _, m := range members {
		key := cluster + "/" + fmt.Sprint(m.NodeID)

		if !m.Trusted {
			// An unreadable member is the dangerous case: it may be far behind
			// and we cannot see how far. Hold at what it last reported, and let
			// the bound below decide when to give up on it.
			if l, known := t.laggards[key]; known {
				if !t.strandedByBound(key, l, snapshotPosition, now) {
					if slowest < 0 || l.position < slowest {
						slowest, slowestNode = l.position, m.NodeID
					}
				}
			} else {
				// Never seen it report: start its clock from now at the current
				// snapshot, so it gets the full bound before being written off.
				t.laggards[key] = &laggard{since: now, position: snapshotPosition}
			}
			continue
		}

		if m.Position >= snapshotPosition {
			// Caught up: it stops being a laggard and its clock resets, so a
			// member that falls behind repeatedly gets the full bound each time
			// rather than accumulating toward being declared stranded.
			delete(t.laggards, key)
			delete(t.stranded, key)
			continue
		}

		l, known := t.laggards[key]
		if !known || l.position != m.Position {
			// Progress, even slow progress, restarts the clock: the bound is for
			// a member that is not catching up, not one that is merely behind.
			l = &laggard{since: now, position: m.Position}
			t.laggards[key] = l
			delete(t.stranded, key)
		}

		if t.strandedByBound(key, l, snapshotPosition, now) {
			continue
		}
		if slowest < 0 || m.Position < slowest {
			slowest, slowestNode = m.Position, m.NodeID
		}
	}

	if slowest >= 0 && slowest < w.Position {
		w.Position = slowest
		w.Limiter = fmt.Sprintf("member node%d", slowestNode)
		w.Detail["slowestMember"] = slowest
	}

	for key, isStranded := range t.stranded {
		if !isStranded {
			continue
		}
		var node int
		if _, err := fmt.Sscanf(key, cluster+"/%d", &node); err == nil {
			w.Stranded = append(w.Stranded, node)
		}
	}

	return w
}

// strandedByBound decides whether a member has held the watermark long enough
// to be given up on, and records the verdict.
//
// Marking is sticky until the member makes progress: a stranded member that
// stays stranded should not flap in and out of the watermark, because that
// would make reclamation depend on which poll the operator happened to read.
func (t *WatermarkTracker) strandedByBound(key string, l *laggard, snapshotPosition int64,
	now time.Time) bool {

	if t.stranded[key] {
		return true
	}
	lagBytes := snapshotPosition - l.position
	if lagBytes >= t.maxLagBytes || now.Sub(l.since) >= t.maxLagDuration {
		t.stranded[key] = true
		return true
	}
	return false
}

// StrandedMembers lists members currently written off, for status and metrics.
func (t *WatermarkTracker) StrandedMembers() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.stranded))
	for key, isStranded := range t.stranded {
		if isStranded {
			out = append(out, key)
		}
	}
	return out
}
