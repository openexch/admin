// SPDX-License-Identifier: Apache-2.0
package services

import (
	"testing"
	"time"

	"github.com/openexch/admin/config"
	"github.com/openexch/admin/logging"
)

func caughtUp(nodes int, at int64) []MemberPosition {
	out := make([]MemberPosition, nodes)
	for i := range out {
		out[i] = MemberPosition{NodeID: i, Position: at, Trusted: true}
	}
	return out
}

// With every consumer past the snapshot, the snapshot is the ceiling: recovery
// on each node needs everything above its own newest snapshot.
func TestSnapshotIsTheCeiling(t *testing.T) {
	tr := NewWatermarkTracker()
	w := tr.Compute("assets", 1000, caughtUp(3, 1000), 2000, 3000)

	if w.Position != 1000 {
		t.Errorf("position = %d, want the snapshot at 1000", w.Position)
	}
	if w.Limiter != "snapshot" {
		t.Errorf("limiter = %q, want snapshot", w.Limiter)
	}
}

// S3 not having stored a range yet is the case step 01 exists to create, and
// purging past it would delete data that never left the box.
func TestS3HoldsTheWatermarkDown(t *testing.T) {
	tr := NewWatermarkTracker()
	w := tr.Compute("assets", 1000, caughtUp(3, 1000), 600, -1)

	if w.Position != 600 {
		t.Errorf("position = %d, want 600 (S3 has not stored past that)", w.Position)
	}
	if w.Limiter != "s3Verified" {
		t.Errorf("limiter = %q, want s3Verified", w.Limiter)
	}
}

// A consumer that has stored NOTHING is not a consumer waiting for data, it is
// one that is not participating. Treating -1 as a constraint would mean a box
// with no bucket configured could never reclaim anything — which is how a
// safety mechanism becomes the outage it was meant to prevent.
func TestAbsentConsumersDoNotPinTheWatermark(t *testing.T) {
	tr := NewWatermarkTracker()
	w := tr.Compute("assets", 1000, caughtUp(3, 1000), -1, -1)

	if w.Position != 1000 {
		t.Errorf("position = %d, want 1000: an absent consumer must not pin it", w.Position)
	}
}

func TestSlowestMemberHoldsTheWatermarkDown(t *testing.T) {
	tr := NewWatermarkTracker()
	members := []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 1000, Trusted: true},
		{NodeID: 2, Position: 400, Trusted: true},
	}

	w := tr.Compute("assets", 1000, members, 2000, -1)

	if w.Position != 400 {
		t.Errorf("position = %d, want 400 (node2 is behind)", w.Position)
	}
	if w.Limiter != "member node2" {
		t.Errorf("limiter = %q, want it to name node2", w.Limiter)
	}
}

// Aeron's CnC counters survive the process that wrote them, so a dead node
// reports a stale position that looks alive. Advancing past it would strand the
// member for real.
func TestUntrustedMemberIsNotAssumedCaughtUp(t *testing.T) {
	tr := NewWatermarkTracker()

	// First observation while healthy and behind.
	behind := []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 500, Trusted: true},
	}
	tr.Compute("assets", 1000, behind, -1, -1)

	// Now it goes unreadable. Its last known position must still hold.
	gone := []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 999999, Trusted: false}, // stale counter reads high
	}
	w := tr.Compute("assets", 1000, gone, -1, -1)

	if w.Position != 500 {
		t.Errorf("position = %d, want 500: a stale counter must not advance the watermark", w.Position)
	}
}

// The bound is required, not optional. Without it one member that is far behind
// holds the watermark down forever, the log grows, and the disk fills anyway —
// the outage returns through the mechanism meant to prevent it.
func TestByteBoundStrandsAMemberThatIsTooFarBehind(t *testing.T) {
	tr := NewWatermarkTracker()
	snapshot := int64(10 << 30) // 10 GiB in
	members := []MemberPosition{
		{NodeID: 0, Position: snapshot, Trusted: true},
		{NodeID: 1, Position: snapshot - (3 << 30), Trusted: true}, // 3 GiB behind, bound is 2
	}

	w := tr.Compute("assets", snapshot, members, -1, -1)

	if w.Position != snapshot {
		t.Errorf("position = %d, want the snapshot: a stranded member stops holding it", w.Position)
	}
	if len(w.Stranded) != 1 || w.Stranded[0] != 1 {
		t.Errorf("stranded = %v, want [1]", w.Stranded)
	}
}

func TestTimeBoundStrandsAMemberThatStopsProgressing(t *testing.T) {
	tr := NewWatermarkTracker()
	tr.maxLagDuration = 50 * time.Millisecond

	members := []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 400, Trusted: true},
	}

	if w := tr.Compute("assets", 1000, members, -1, -1); w.Position != 400 {
		t.Fatalf("position = %d, want 400 before the bound elapses", w.Position)
	}

	time.Sleep(60 * time.Millisecond)

	w := tr.Compute("assets", 1000, members, -1, -1)
	if w.Position != 1000 {
		t.Errorf("position = %d, want 1000 after the bound elapsed", w.Position)
	}
	if len(w.Stranded) != 1 {
		t.Errorf("stranded = %v, want node1 written off", w.Stranded)
	}
}

// The bound is for a member that is NOT catching up. One that is merely slow
// must keep its place — writing off a member that is making progress forces an
// unnecessary reseed of the money ledger.
func TestProgressRestartsTheClock(t *testing.T) {
	tr := NewWatermarkTracker()
	tr.maxLagDuration = 50 * time.Millisecond

	tr.Compute("assets", 1000, []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 400, Trusted: true},
	}, -1, -1)

	time.Sleep(40 * time.Millisecond)

	// It moved: clock restarts.
	tr.Compute("assets", 1000, []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 700, Trusted: true},
	}, -1, -1)

	time.Sleep(40 * time.Millisecond) // 80ms total, but only 40ms since it moved

	w := tr.Compute("assets", 1000, []MemberPosition{
		{NodeID: 0, Position: 1000, Trusted: true},
		{NodeID: 1, Position: 700, Trusted: true},
	}, -1, -1)

	if w.Position != 700 {
		t.Errorf("position = %d, want 700: a member making progress keeps its place", w.Position)
	}
	if len(w.Stranded) != 0 {
		t.Errorf("stranded = %v, want none", w.Stranded)
	}
}

// A member that catches up must stop being stranded, or a one-off lag would
// permanently mark a healthy node and mislead every operator who looks after.
func TestCatchingUpClearsStranded(t *testing.T) {
	tr := NewWatermarkTracker()
	snapshot := int64(10 << 30)

	tr.Compute("assets", snapshot, []MemberPosition{
		{NodeID: 0, Position: snapshot, Trusted: true},
		{NodeID: 1, Position: snapshot - (3 << 30), Trusted: true},
	}, -1, -1)

	if len(tr.StrandedMembers()) != 1 {
		t.Fatalf("expected node1 stranded, got %v", tr.StrandedMembers())
	}

	w := tr.Compute("assets", snapshot, caughtUp(2, snapshot), -1, -1)

	if len(w.Stranded) != 0 || len(tr.StrandedMembers()) != 0 {
		t.Errorf("stranded = %v / %v, want cleared once caught up", w.Stranded, tr.StrandedMembers())
	}
}

// The operator asking "why is the disk not shrinking?" needs the answer in the
// data, not in a reconstruction.
func TestDetailCarriesEveryInput(t *testing.T) {
	tr := NewWatermarkTracker()
	w := tr.Compute("assets", 1000, []MemberPosition{
		{NodeID: 0, Position: 800, Trusted: true},
	}, 900, 950)

	for _, key := range []string{"snapshot", "s3Verified", "backup", "slowestMember"} {
		if _, ok := w.Detail[key]; !ok {
			t.Errorf("detail is missing %q: %v", key, w.Detail)
		}
	}
	if w.Position != 800 {
		t.Errorf("position = %d, want 800", w.Position)
	}
}

// The calculation and the application are split so the numbers can be watched
// under real traffic before anything starts deleting on their strength. With
// the flag off the purge must receive "no constraint" (-1), NOT the computed
// position — and certainly not zero, which would stop reclamation dead.
func TestWatermarkIsComputedButNotAppliedUntilEnabled(t *testing.T) {
	statusSvc := &StatusService{
		cachedStatus: map[string]interface{}{
			"clusters": []map[string]interface{}{
				{
					"name": "assets",
					"nodes": []map[string]interface{}{
						{"id": 0, "commitPosition": int64(400), "health": HealthHealthy},
						{"id": 1, "commitPosition": int64(1000), "health": HealthHealthy},
					},
				},
			},
		},
		lastUpdate: time.Now(),
	}

	newOps := func(apply bool) *OperationsService {
		o := &OperationsService{
			cfg:      &config.Config{WatermarkRetention: apply},
			cluster:  &Cluster{Name: "assets", NodePrefix: "ae"},
			progress: NewProgress(),
			log:      logging.Component("test"),
		}
		o.statusSvc = statusSvc
		o.watermarks = NewWatermarkTracker()
		return o
	}

	off := newOps(false)
	if got := off.retentionWatermark(off.log, 1000); got != -1 {
		t.Errorf("with the flag off the purge got %d, want -1 (no constraint)", got)
	}

	on := newOps(true)
	if got := on.retentionWatermark(on.log, 1000); got != 400 {
		t.Errorf("with the flag on the purge got %d, want the slowest member at 400", got)
	}
}

// No tracker at all must mean the old behaviour, not a zero that would stop
// reclamation on every cluster that has not been wired up.
func TestNoTrackerMeansNoConstraint(t *testing.T) {
	o := &OperationsService{
		cfg:      &config.Config{WatermarkRetention: true},
		cluster:  &Cluster{Name: "assets"},
		progress: NewProgress(),
		log:      logging.Component("test"),
	}

	if got := o.retentionWatermark(o.log, 1000); got != -1 {
		t.Errorf("unwired tracker yielded %d, want -1", got)
	}
}
