// SPDX-License-Identifier: Apache-2.0
package services

import (
	"encoding/binary"
	"os"
)

// Aeron's cluster recording log, read directly instead of through ClusterTool.
//
// `ClusterTool recording-log` spawns a JVM per node per poll. That cost is why
// the positions it yields were gated behind RichArchiveStats, and why the
// Assets Engine reported no snapshot position at all: the console could not
// answer "is the money ledger snapshotting?" during the very hours it was
// filling /dev/shm. This is the same shape as the archive-size gate removed in
// #106 — the number that mattered was hidden behind the cost of fetching it.
//
// The file is a flat array of fixed-size little-endian records, so reading it
// in-process costs microseconds and no process spawn. Every cluster can afford
// it, and the matching engine loses three JVM spawns per poll.
//
// Layout mirrors io.aeron.cluster.RecordingLog (Aeron 1.52.2). Verified against
// a production file: recordinglog_test.go asserts this parser against the same
// bytes ClusterTool itself read, entry for entry.
const (
	recLogRecordingIDOffset         = 0
	recLogLeadershipTermIDOffset    = 8
	recLogTermBaseLogPositionOffset = 16
	recLogLogPositionOffset         = 24
	recLogTimestampOffset           = 32
	recLogServiceIDOffset           = 40
	recLogEntryTypeOffset           = 44

	// A TERM or SNAPSHOT entry ends here; a STANDBY_SNAPSHOT continues with a
	// length-prefixed archive endpoint. Every entry is then padded to alignment.
	recLogEndpointOffset = 48
	recLogAlignment      = 64

	recLogTypeTerm            = 0
	recLogTypeSnapshot        = 1
	recLogTypeStandbySnapshot = 2

	// Aeron marks an entry logically removed by OR-ing this into the type field
	// rather than rewriting the file.
	recLogInvalidFlag uint32 = 1 << 31

	// Aeron's NULL_VALUE: an in-progress leadership term has no end position yet.
	recLogNullPosition int64 = -1

	// A single entry can hold an endpoint at most this long. Anything larger is
	// a corrupt length prefix, not a record — stop rather than seek off a cliff.
	recLogMaxEndpointLen = 1024
)

// RecordingLogEntry is one record of the cluster recording log.
type RecordingLogEntry struct {
	RecordingID         int64
	LeadershipTermID    int64
	TermBaseLogPosition int64
	LogPosition         int64
	Timestamp           int64
	ServiceID           int32
	Type                int32
	IsValid             bool
}

// ParseRecordingLog decodes every entry in a cluster recording.log.
//
// A short or corrupt tail ends the scan and returns the entries read so far:
// the file is append-only and read without locking, so observing a partially
// written trailing record is expected, not an error.
func ParseRecordingLog(data []byte) []RecordingLogEntry {
	entries := make([]RecordingLogEntry, 0, len(data)/recLogAlignment)

	for off := 0; off+recLogEndpointOffset <= len(data); {
		rawType := binary.LittleEndian.Uint32(data[off+recLogEntryTypeOffset:])
		entryType := int32(rawType &^ recLogInvalidFlag)

		entries = append(entries, RecordingLogEntry{
			RecordingID:         int64(binary.LittleEndian.Uint64(data[off+recLogRecordingIDOffset:])),
			LeadershipTermID:    int64(binary.LittleEndian.Uint64(data[off+recLogLeadershipTermIDOffset:])),
			TermBaseLogPosition: int64(binary.LittleEndian.Uint64(data[off+recLogTermBaseLogPositionOffset:])),
			LogPosition:         int64(binary.LittleEndian.Uint64(data[off+recLogLogPositionOffset:])),
			Timestamp:           int64(binary.LittleEndian.Uint64(data[off+recLogTimestampOffset:])),
			ServiceID:           int32(binary.LittleEndian.Uint32(data[off+recLogServiceIDOffset:])),
			Type:                entryType,
			IsValid:             rawType&recLogInvalidFlag == 0,
		})

		length := recLogAlignment
		if entryType == recLogTypeStandbySnapshot {
			// Variable length: the endpoint's length prefix decides the stride.
			if off+recLogEndpointOffset+4 > len(data) {
				break
			}
			endpointLen := int(int32(binary.LittleEndian.Uint32(data[off+recLogEndpointOffset:])))
			if endpointLen < 0 || endpointLen > recLogMaxEndpointLen {
				break
			}
			length = align(recLogEndpointOffset+4+endpointLen, recLogAlignment)
		}
		off += length
	}

	return entries
}

func align(value, alignment int) int {
	return (value + alignment - 1) &^ (alignment - 1)
}

// ReadRecordingLog parses the recording log of a node's cluster directory.
// A missing file (node never started) is not an error — it yields no entries.
func ReadRecordingLog(clusterDir string) ([]RecordingLogEntry, error) {
	data, err := os.ReadFile(clusterDir + "/recording.log")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ParseRecordingLog(data), nil
}

// LatestPositions reduces the log to the two numbers the console shows.
//
// Both ignore invalidated entries, which the previous ClusterTool text scrape
// could not: it regex-matched every printed logPosition, so an invalidated
// snapshot still counted as a restore point. An invalidated entry is one Aeron
// has logically removed, and treating it as recoverable state is exactly the
// kind of optimistic number this system must never report.
//
// Returns -1 for a value the log does not carry, matching the tool-era contract
// (no entries at all, or a log with no snapshot yet).
func LatestPositions(entries []RecordingLogEntry) (logPosition, snapshotPosition int64) {
	logPosition, snapshotPosition = -1, -1

	for _, e := range entries {
		if !e.IsValid || e.LogPosition == recLogNullPosition {
			// An in-progress term carries NULL_VALUE until it is committed.
			continue
		}
		if e.LogPosition > logPosition {
			logPosition = e.LogPosition
		}
		if e.Type == recLogTypeSnapshot && e.LogPosition > snapshotPosition {
			snapshotPosition = e.LogPosition
		}
	}

	return logPosition, snapshotPosition
}
