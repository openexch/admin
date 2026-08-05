// SPDX-License-Identifier: Apache-2.0
package services

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/openexch/admin/config"
)

// testdata/recording-log-ae2.bin is a PRODUCTION file: the Assets Engine node
// ae2's recording.log, copied off the demo box on 2026-07-26 with the live file
// verified byte-identical before and after the copy. The expectations below are
// ClusterTool's own output over those exact bytes, so this test is a real
// cross-check against Aeron's reader rather than a restatement of the parser.
const (
	ae2Entries          = 56
	ae2ValidSnapshots   = 34
	ae2MaxLogPosition   = 1272550400
	ae2SnapshotPosition = 1272550400
)

func loadFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "recording-log-ae2.bin"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

func TestParseRecordingLogMatchesClusterTool(t *testing.T) {
	entries := ParseRecordingLog(loadFixture(t))

	if len(entries) != ae2Entries {
		t.Fatalf("entries = %d, ClusterTool reported %d", len(entries), ae2Entries)
	}

	// Entry 0, field for field:
	// Entry{recordingId=0, leadershipTermId=0, termBaseLogPosition=0,
	//       logPosition=96, timestamp=1785017156861, serviceId=-1,
	//       type=TERM, isValid=true, entryIndex=0}
	first := entries[0]
	if first.RecordingID != 0 || first.LeadershipTermID != 0 || first.TermBaseLogPosition != 0 ||
		first.LogPosition != 96 || first.Timestamp != 1785017156861 ||
		first.ServiceID != -1 || first.Type != recLogTypeTerm || !first.IsValid {
		t.Errorf("first entry = %+v, want ClusterTool's entryIndex=0", first)
	}

	// Entry 55: Entry{recordingId=35, leadershipTermId=21,
	//       termBaseLogPosition=1228918592, logPosition=1272550400,
	//       timestamp=1785023295829, serviceId=-1, type=SNAPSHOT, isValid=true}
	last := entries[len(entries)-1]
	if last.RecordingID != 35 || last.LeadershipTermID != 21 || last.TermBaseLogPosition != 1228918592 ||
		last.LogPosition != ae2SnapshotPosition || last.Timestamp != 1785023295829 ||
		last.ServiceID != -1 || last.Type != recLogTypeSnapshot || !last.IsValid {
		t.Errorf("last entry = %+v, want ClusterTool's entryIndex=55", last)
	}

	snapshots := 0
	for _, e := range entries {
		if e.Type == recLogTypeSnapshot && e.IsValid {
			snapshots++
		}
	}
	if snapshots != ae2ValidSnapshots {
		t.Errorf("valid snapshot entries = %d, ClusterTool reported %d", snapshots, ae2ValidSnapshots)
	}

	// Terms chain: each term's base is the previous term's end. This is the
	// property that proves the stride is right — a wrong entry length would
	// still decode plausible-looking numbers, but they would not chain.
	var prevTermEnd int64 = -1
	for _, e := range entries {
		if e.Type != recLogTypeTerm || !e.IsValid {
			continue
		}
		if prevTermEnd >= 0 && e.TermBaseLogPosition != prevTermEnd {
			t.Errorf("term %d base = %d, previous term ended at %d",
				e.LeadershipTermID, e.TermBaseLogPosition, prevTermEnd)
		}
		if e.LogPosition != recLogNullPosition {
			prevTermEnd = e.LogPosition
		}
	}
}

func TestLatestPositionsFromProductionLog(t *testing.T) {
	logPos, snapPos := LatestPositions(ParseRecordingLog(loadFixture(t)))

	if logPos != ae2MaxLogPosition {
		t.Errorf("logPosition = %d, want %d", logPos, ae2MaxLogPosition)
	}
	if snapPos != ae2SnapshotPosition {
		t.Errorf("snapshotPosition = %d, want %d", snapPos, ae2SnapshotPosition)
	}
}

// --- synthetic entries, for the cases production has not produced yet ---

type testEntry struct {
	recordingID   int64
	termID        int64
	termBase      int64
	logPosition   int64
	timestamp     int64
	serviceID     int32
	entryType     int32
	invalid       bool
	standbyTarget string
}

func encodeEntries(entries []testEntry) []byte {
	var out []byte
	for _, e := range entries {
		length := recLogAlignment
		if e.entryType == recLogTypeStandbySnapshot {
			length = align(recLogEndpointOffset+4+len(e.standbyTarget), recLogAlignment)
		}
		buf := make([]byte, length)
		binary.LittleEndian.PutUint64(buf[recLogRecordingIDOffset:], uint64(e.recordingID))
		binary.LittleEndian.PutUint64(buf[recLogLeadershipTermIDOffset:], uint64(e.termID))
		binary.LittleEndian.PutUint64(buf[recLogTermBaseLogPositionOffset:], uint64(e.termBase))
		binary.LittleEndian.PutUint64(buf[recLogLogPositionOffset:], uint64(e.logPosition))
		binary.LittleEndian.PutUint64(buf[recLogTimestampOffset:], uint64(e.timestamp))
		binary.LittleEndian.PutUint32(buf[recLogServiceIDOffset:], uint32(e.serviceID))

		rawType := uint32(e.entryType)
		if e.invalid {
			rawType |= recLogInvalidFlag
		}
		binary.LittleEndian.PutUint32(buf[recLogEntryTypeOffset:], rawType)

		if e.entryType == recLogTypeStandbySnapshot {
			binary.LittleEndian.PutUint32(buf[recLogEndpointOffset:], uint32(len(e.standbyTarget)))
			copy(buf[recLogEndpointOffset+4:], e.standbyTarget)
		}
		out = append(out, buf...)
	}
	return out
}

// An invalidated snapshot is one Aeron has logically removed. Reporting it as
// the latest restore point would overstate what is recoverable — the exact kind
// of optimistic number that let a dead money path look healthy for 17 hours.
func TestInvalidatedSnapshotIsNotARestorePoint(t *testing.T) {
	data := encodeEntries([]testEntry{
		{termID: 1, logPosition: 500, entryType: recLogTypeTerm},
		{recordingID: 1, termID: 1, logPosition: 400, serviceID: -1, entryType: recLogTypeSnapshot},
		{recordingID: 2, termID: 1, logPosition: 900, serviceID: -1, entryType: recLogTypeSnapshot, invalid: true},
	})

	entries := ParseRecordingLog(data)
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	if entries[2].IsValid {
		t.Error("entry 2 carries the invalid flag but decoded as valid")
	}
	if entries[2].Type != recLogTypeSnapshot {
		t.Errorf("entry 2 type = %d, want SNAPSHOT with the flag masked off", entries[2].Type)
	}

	logPos, snapPos := LatestPositions(entries)
	if snapPos != 400 {
		t.Errorf("snapshotPosition = %d, want 400 (the invalidated 900 must not count)", snapPos)
	}
	if logPos != 500 {
		t.Errorf("logPosition = %d, want 500 (the invalidated entry must not count)", logPos)
	}
}

// The active leadership term carries NULL_VALUE until it is committed. Treating
// it as a number would report a position of -1 as the newest log position.
func TestActiveTermNullPositionIgnored(t *testing.T) {
	data := encodeEntries([]testEntry{
		{termID: 1, logPosition: 700, entryType: recLogTypeTerm},
		{termID: 2, termBase: 700, logPosition: recLogNullPosition, entryType: recLogTypeTerm},
	})

	logPos, snapPos := LatestPositions(ParseRecordingLog(data))
	if logPos != 700 {
		t.Errorf("logPosition = %d, want 700", logPos)
	}
	if snapPos != -1 {
		t.Errorf("snapshotPosition = %d, want -1 (no snapshot yet)", snapPos)
	}
}

// A standby-snapshot entry is longer than the others. Assuming a fixed stride
// would desynchronise every entry after it.
func TestStandbySnapshotVariableLengthKeepsStride(t *testing.T) {
	data := encodeEntries([]testEntry{
		{recordingID: 1, termID: 1, logPosition: 100, entryType: recLogTypeStandbySnapshot,
			standbyTarget: "aeron:udp?endpoint=standby.example.internal:8010"},
		{recordingID: 2, termID: 1, logPosition: 200, serviceID: -1, entryType: recLogTypeSnapshot},
	})

	entries := ParseRecordingLog(data)
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2 (stride desynchronised)", len(entries))
	}
	if entries[1].RecordingID != 2 || entries[1].LogPosition != 200 {
		t.Errorf("entry after standby snapshot = %+v, want recordingId=2 logPosition=200", entries[1])
	}
	if _, snapPos := LatestPositions(entries); snapPos != 200 {
		t.Errorf("snapshotPosition = %d, want 200 (a standby snapshot is not a local restore point)", snapPos)
	}
}

// The log is append-only and read without locking, so a half-written trailing
// record is normal. It must not lose the entries already read, or panic.
func TestPartialTrailingRecordIsIgnored(t *testing.T) {
	data := encodeEntries([]testEntry{
		{termID: 1, logPosition: 300, entryType: recLogTypeTerm},
		{recordingID: 1, termID: 1, logPosition: 250, serviceID: -1, entryType: recLogTypeSnapshot},
	})
	truncated := data[:len(data)-20]

	entries := ParseRecordingLog(truncated)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1 complete entry", len(entries))
	}
	if logPos, _ := LatestPositions(entries); logPos != 300 {
		t.Errorf("logPosition = %d, want 300", logPos)
	}
}

func TestEmptyAndMissingLog(t *testing.T) {
	if entries := ParseRecordingLog(nil); len(entries) != 0 {
		t.Errorf("nil data yielded %d entries", len(entries))
	}
	if logPos, snapPos := LatestPositions(nil); logPos != -1 || snapPos != -1 {
		t.Errorf("empty log = (%d, %d), want (-1, -1)", logPos, snapPos)
	}

	// A node that has never started has no recording.log. That is a state, not
	// a failure: the console shows "--" rather than an error.
	entries, err := ReadRecordingLog(filepath.Join(t.TempDir(), "no-such-node"))
	if err != nil {
		t.Errorf("missing recording.log returned error %v, want nil", err)
	}
	if len(entries) != 0 {
		t.Errorf("missing recording.log yielded %d entries", len(entries))
	}
}

// The Assets Engine is the "lean" cluster (RichArchiveStats false). Its restore
// point must come back anyway: gating these numbers is what left the money
// ledger's snapshot position blank in the console while it filled /dev/shm.
func TestLeanClusterStillReportsItsRestorePoint(t *testing.T) {
	stateDir := t.TempDir()
	assets := NewAssetsCluster(&config.Config{AssetsStateDir: stateDir})

	if assets.RichArchiveStats {
		t.Fatal("assets is expected to be the lean cluster — this test asserts the lean path")
	}

	clusterDir := filepath.Join(assets.NodeStateDir(0), "cluster")
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(clusterDir, "recording.log"), loadFixture(t), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	logPos, snapPos := assets.GetLogAndSnapshotPositions(0)
	if logPos != ae2MaxLogPosition {
		t.Errorf("logPosition = %d, want %d", logPos, ae2MaxLogPosition)
	}
	if snapPos != ae2SnapshotPosition {
		t.Errorf("snapshotPosition = %d, want %d", snapPos, ae2SnapshotPosition)
	}
	if got := assets.GetSnapshotPosition(0); got != ae2SnapshotPosition {
		t.Errorf("GetSnapshotPosition = %d, want %d", got, ae2SnapshotPosition)
	}
}

// A node that has never run reports -1, not a zero that would read as "the log
// is fully snapshotted".
func TestNodeWithoutRecordingLogReportsUnknown(t *testing.T) {
	assets := NewAssetsCluster(&config.Config{AssetsStateDir: t.TempDir()})

	logPos, snapPos := assets.GetLogAndSnapshotPositions(0)
	if logPos != -1 || snapPos != -1 {
		t.Errorf("positions = (%d, %d), want (-1, -1)", logPos, snapPos)
	}
}

func TestReadRecordingLogFromDisk(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "recording.log"), loadFixture(t), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	entries, err := ReadRecordingLog(dir)
	if err != nil {
		t.Fatalf("ReadRecordingLog: %v", err)
	}
	if len(entries) != ae2Entries {
		t.Errorf("entries = %d, want %d", len(entries), ae2Entries)
	}
}
