// SPDX-License-Identifier: Apache-2.0
package services

import "time"

// The seam between this admin core and the operations built on top of it.
//
// The core runs and survives ONE cluster on ONE box: supervision, status,
// health, snapshots, retention, cleanup, recovery. Everything that coordinates
// across boxes, across time or across an artifact pipeline lives above it and
// injects itself here.
//
// The rule these interfaces exist to protect: durability stays in the core. A
// box with nothing plugged in still snapshots on a byte trigger, still purges
// its log under a watermark, and still survives a node dying. The off-box half
// makes that durable somewhere else; it never becomes a precondition for it.
// Anything that inverts this makes the open claim untrue.

// LedgerArchiver is the off-box half of the ledger archive: it takes the
// bundles a snapshot produces and reports how far the durable copy has been
// verified.
//
// The core reads that position for one purpose only — as a POSSIBLE limiter on
// how far the log may be reclaimed. A negative position means "not
// participating", never "reclaim nothing" (see WatermarkTracker.Compute), so a
// box with no archive plugged in reclaims exactly as it always did.
type LedgerArchiver interface {
	// VerifiedPosition is the log position whose durable copy is confirmed, or
	// a negative number when this cluster has none.
	VerifiedPosition(cluster string) int64
	// DurableSnapshotAt is when the most recent verified snapshot was taken,
	// or the zero time when there is none. Evidence that outlives local state.
	DurableSnapshotAt(cluster string) time.Time
	// ToMap renders the archive's state for /api/admin/status.
	ToMap() map[string]interface{}
}

// NoLedgerArchive is the core's default: nothing is shipped off the box and
// nothing is claimed to be durable elsewhere. It is a value rather than a nil
// interface so that every call site stays a plain method call — a nil check
// that someone forgets is a panic in the snapshot path, which is the one path
// that must not have any.
var NoLedgerArchive LedgerArchiver = noLedgerArchive{}

type noLedgerArchive struct{}

func (noLedgerArchive) VerifiedPosition(string) int64 { return -1 }

func (noLedgerArchive) DurableSnapshotAt(string) time.Time { return time.Time{} }

func (noLedgerArchive) ToMap() map[string]interface{} {
	return map[string]interface{}{"enabled": false}
}
