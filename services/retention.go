// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"log/slog"
)

// retentionWatermark computes how far this cluster's log is reclaimable, and
// reports it whether or not it is being applied.
//
// Reporting first, applying second, on purpose. The watermark replaces a
// boolean that has exactly two wrong answers, so it is a genuine improvement -
// but it also changes what gets DELETED from a live money ledger, and the one
// direction that hurts is deleting something a member still needed. Computing
// it under real traffic and reading the numbers costs nothing; being wrong
// costs a reseed of the ledger.
//
// Returns -1 when it cannot be computed, which callers must treat as "no
// external constraint" rather than "purge nothing".
func (o *OperationsService) retentionWatermark(log *slog.Logger, snapshotPosition int64) int64 {
	if o.watermarks == nil || o.statusSvc == nil {
		return -1
	}

	_, nodes := o.clusterHealthFromStatus(o.statusSvc.GetStatus())
	members := make([]MemberPosition, 0, len(nodes))
	for _, n := range nodes {
		id, ok := n["id"].(int)
		if !ok {
			continue
		}
		pos, hasPos := n["commitPosition"].(int64)
		health, _ := n["health"].(string)
		members = append(members, MemberPosition{
			NodeID:   id,
			Position: pos,
			// A dead node's CnC counters outlive it and read high, so only a
			// member we believe is alive gets its reported position trusted.
			Trusted: hasPos && health == HealthHealthy,
		})
	}

	backup := int64(-1) // the AE has no backup service yet; the ME's is not position-addressed
	w := o.watermarks.Compute(o.cluster.Name, snapshotPosition, members,
		o.archiver().VerifiedPosition(o.cluster.Name), backup)

	log.Info("retention watermark", "cluster", o.cluster.Name,
		"position", w.Position, "limiter", w.Limiter, "detail", w.Detail,
		"applied", o.cfg.WatermarkRetention)

	for _, node := range w.Stranded {
		// Loud by design: this is an explicit decision to stop waiting for a
		// member, and the only remedy is a reseed. An accidental disk-full is
		// what happens when nobody says this out loud.
		log.Error("member STRANDED: it held the retention watermark past the bound and has been written off; it cannot log-catch-up and must be reseeded",
			"cluster", o.cluster.Name, "node", fmt.Sprintf("%s%d", o.cluster.NodePrefix, node))
	}

	if !o.cfg.WatermarkRetention {
		return -1
	}
	return w.Position
}
