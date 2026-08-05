// SPDX-License-Identifier: Apache-2.0
package services

import (
	"testing"
	"time"

	"github.com/openexch/admin/logging"
)

// A stand-in for whatever off-box archiver is plugged in above this core. The
// behaviour under test belongs to the reporting path, not to any one uploader.
type fakeArchive struct {
	cluster string
	pos     int64
	at      time.Time
}

func (f fakeArchive) VerifiedPosition(cluster string) int64 {
	if cluster == f.cluster {
		return f.pos
	}
	return -1
}

func (f fakeArchive) DurableSnapshotAt(cluster string) time.Time {
	if cluster == f.cluster {
		return f.at
	}
	return time.Time{}
}

func (f fakeArchive) ToMap() map[string]interface{} {
	return map[string]interface{}{"enabled": true, "s3VerifiedPosition": f.pos}
}

func archiveWithVerified(cluster string, pos int64, at time.Time) LedgerArchiver {
	return fakeArchive{cluster: cluster, pos: pos, at: at}
}

// The whole point: a gateway restarted a minute ago reports zero snapshots of its
// own AND the fact that S3 holds a recent one. Before local bundles were
// reclaimed, an operator could settle this with `ls`; now the only second record
// is this one.
func TestRestartedGatewayStillReportsTheDurableSnapshot(t *testing.T) {
	fresh := &AutoSnapshot{
		enabled:     true,
		targets:     []snapshotTarget{{name: "assets", ops: &OperationsService{}}},
		results:     map[string]*snapshotResult{}, // wiped by the restart
		lastTrigger: map[string]string{},
		clocks:      map[string]*cadenceClock{},
		log:         logging.Component("test"),
		archive:     archiveWithVerified("assets", 1629471680, time.Now().Add(-4*time.Minute)),
	}

	m := fresh.ToMap()
	clusters := m["clusters"].(map[string]interface{})
	assets := clusters["assets"].(map[string]interface{})

	if assets["snapshots"] != 0 {
		t.Errorf("this process has taken no snapshots; it reported %v", assets["snapshots"])
	}
	if _, ok := assets["lastSuccess"]; ok {
		t.Error("claimed a lastSuccess it cannot remember taking")
	}
	if _, ok := assets["durableSnapshotAt"]; !ok {
		t.Fatal("no durable evidence reported, so the panel still reads 'never' after a restart " +
			"and the local bundles that used to answer this are now reclaimed")
	}
	secs, ok := assets["secondsSinceDurableSnapshot"].(int64)
	if !ok || secs < 200 || secs > 300 {
		t.Errorf("secondsSinceDurableSnapshot = %v, want ~240", assets["secondsSinceDurableSnapshot"])
	}
}

// Alongside, never merged. "This process has taken 0 snapshots" and "S3 holds one
// from four minutes ago" are different facts. Folding the second into the first
// would be the gateway taking credit for work it cannot remember, which is
// exactly what keeping the counters in memory was meant to prevent.
func TestDurableEvidenceDoesNotBecomeASuccessCount(t *testing.T) {
	fresh := &AutoSnapshot{
		enabled:     true,
		targets:     []snapshotTarget{{name: "assets", ops: &OperationsService{}}},
		results:     map[string]*snapshotResult{},
		lastTrigger: map[string]string{},
		clocks:      map[string]*cadenceClock{},
		log:         logging.Component("test"),
		archive:     archiveWithVerified("assets", 1629471680, time.Now().Add(-time.Minute)),
	}

	assets := fresh.ToMap()["clusters"].(map[string]interface{})["assets"].(map[string]interface{})
	if got := assets["snapshots"]; got != 0 {
		t.Errorf("durable evidence inflated the success count to %v", got)
	}
	if _, ok := assets["secondsSinceLastSuccess"]; ok {
		t.Error("durable evidence was reported as this process's own last success")
	}
}

// An empty bucket is a real state on a fresh box, and it must not produce a
// timestamp. Reporting one would say "S3 has a snapshot" when it has none, which
// is worse than reporting nothing at all.
func TestNoDurableEvidenceIsReportedWhenS3HasNone(t *testing.T) {
	fresh := &AutoSnapshot{
		enabled:     true,
		targets:     []snapshotTarget{{name: "assets", ops: &OperationsService{}}},
		results:     map[string]*snapshotResult{},
		lastTrigger: map[string]string{},
		clocks:      map[string]*cadenceClock{},
		log:         logging.Component("test"),
		archive:     archiveWithVerified("assets", -1, time.Time{}),
	}

	assets := fresh.ToMap()["clusters"].(map[string]interface{})["assets"].(map[string]interface{})
	if _, ok := assets["durableSnapshotAt"]; ok {
		t.Error("reported a durable snapshot for a cluster S3 has nothing for")
	}
}

// No archive configured leaves the report byte-identical to before. A box with no
// bucket still gets a working panel.
func TestNoArchiveLeavesTheReportUnchanged(t *testing.T) {
	fresh := &AutoSnapshot{
		enabled:     true,
		targets:     []snapshotTarget{{name: "assets", ops: &OperationsService{}}},
		results:     map[string]*snapshotResult{},
		lastTrigger: map[string]string{},
		clocks:      map[string]*cadenceClock{},
		log:         logging.Component("test"),
	}

	assets := fresh.ToMap()["clusters"].(map[string]interface{})["assets"].(map[string]interface{})
	if _, ok := assets["durableSnapshotAt"]; ok {
		t.Error("invented durable evidence with no archive configured")
	}
	if assets["snapshots"] != 0 {
		t.Errorf("snapshots = %v", assets["snapshots"])
	}
}

// No archiver at all must answer rather than panic: it is the single-box case,
// which is the DEFAULT here, and the reporter has no business knowing the
// difference between "nothing plugged in" and "plugged in, nothing durable yet".
func TestNoArchiverAnswersTheDurableQuestion(t *testing.T) {
	if got := NoLedgerArchive.DurableSnapshotAt("assets"); !got.IsZero() {
		t.Errorf("no-op archiver returned %v", got)
	}
	if got := NoLedgerArchive.VerifiedPosition("assets"); got >= 0 {
		t.Errorf("no-op archiver claimed verified position %d; a negative value is "+
			"what tells the watermark it is not participating", got)
	}
	var fresh AutoSnapshot
	if got := fresh.archiver().DurableSnapshotAt("assets"); !got.IsZero() {
		t.Errorf("zero-value reporter returned %v", got)
	}
}

// Both records appear together once the process has done its own work, and they
// are independent numbers rather than copies of each other.
func TestBothRecordsCoexist(t *testing.T) {
	own := time.Now().Add(-30 * time.Second)
	durable := time.Now().Add(-5 * time.Minute)

	a := &AutoSnapshot{
		enabled: true,
		targets: []snapshotTarget{{name: "assets", ops: &OperationsService{}}},
		results: map[string]*snapshotResult{
			"assets": {Successes: 3, LastSuccessAt: own},
		},
		lastTrigger: map[string]string{},
		clocks:      map[string]*cadenceClock{},
		log:         logging.Component("test"),
		archive:     archiveWithVerified("assets", 1629471680, durable),
	}

	assets := a.ToMap()["clusters"].(map[string]interface{})["assets"].(map[string]interface{})
	if assets["snapshots"] != 3 {
		t.Errorf("snapshots = %v, want 3", assets["snapshots"])
	}
	if assets["lastSuccess"] != own.UTC().Format(time.RFC3339) {
		t.Errorf("lastSuccess = %v", assets["lastSuccess"])
	}
	if assets["durableSnapshotAt"] != durable.UTC().Format(time.RFC3339) {
		t.Errorf("durableSnapshotAt = %v", assets["durableSnapshotAt"])
	}
	if assets["lastSuccess"] == assets["durableSnapshotAt"] {
		t.Error("the two records are the same value; one is being copied from the other")
	}
}
