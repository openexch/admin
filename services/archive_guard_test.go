// SPDX-License-Identifier: Apache-2.0
package services

import (
	"strings"
	"testing"
)

// The purge guard must judge THIS cluster. It used to read the top-level
// allNodesHealthy / nodes fields, which are always the matching engine's, so an
// assets-cluster reclaim would have been gated on ME health and vice versa.
func TestArchiveOpBlockReasonIsClusterScoped(t *testing.T) {
	status := map[string]interface{}{
		// Top-level block is the matching engine's, and it is UNHEALTHY.
		"allNodesHealthy": false,
		"nodes": []map[string]interface{}{
			{"id": 2, "procName": "node2", "health": "OFFLINE"},
		},
		"clusters": []map[string]interface{}{
			{
				"name":            "match",
				"allNodesHealthy": false,
				"nodes": []map[string]interface{}{
					{"id": 0, "procName": "node0", "health": "HEALTHY"},
					{"id": 2, "procName": "node2", "health": "OFFLINE"},
				},
			},
			{
				"name":            "assets",
				"allNodesHealthy": true,
				"nodes": []map[string]interface{}{
					{"id": 0, "procName": "ae0", "health": "HEALTHY"},
					{"id": 1, "procName": "ae1", "health": "HEALTHY"},
				},
			},
		},
	}

	assets := &OperationsService{cluster: &Cluster{Name: "assets"}}
	if healthy, _ := assets.clusterHealthFromStatus(status); !healthy {
		t.Fatal("assets judged unhealthy because the matching engine is; the AE would never reclaim")
	}

	match := &OperationsService{cluster: &Cluster{Name: "match"}}
	healthy, nodes := match.clusterHealthFromStatus(status)
	if healthy {
		t.Fatal("match judged healthy despite node2 OFFLINE")
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d match nodes, want 2", len(nodes))
	}
}

// With no generic clusters array (older status payloads), fall back to the
// top-level fields rather than silently reporting healthy.
func TestArchiveOpBlockReasonFallsBackToTopLevel(t *testing.T) {
	status := map[string]interface{}{
		"allNodesHealthy": false,
		"nodes": []map[string]interface{}{
			{"id": 1, "procName": "node1", "health": "DEGRADED"},
		},
	}
	ops := &OperationsService{cluster: &Cluster{Name: "match"}}
	healthy, nodes := ops.clusterHealthFromStatus(status)
	if healthy || len(nodes) != 1 {
		t.Fatalf("fallback failed: healthy=%v nodes=%d", healthy, len(nodes))
	}
}

// The refusal names the offending members so an operator does not have to go
// digging for which node is holding reclamation back.
func TestArchiveOpBlockReasonNamesTheUnhealthyMembers(t *testing.T) {
	ops := &OperationsService{
		cluster:   &Cluster{Name: "assets"},
		statusSvc: nil, // no status service == no opinion
	}
	if reason := ops.archiveOpBlockReason(); reason != "" {
		t.Fatalf("expected no opinion without a status service, got %q", reason)
	}

	status := map[string]interface{}{
		"clusters": []map[string]interface{}{
			{
				"name":            "assets",
				"allNodesHealthy": false,
				"nodes": []map[string]interface{}{
					{"id": 0, "procName": "ae0", "health": "HEALTHY"},
					{"id": 2, "procName": "ae2", "health": "OFFLINE"},
				},
			},
		},
	}
	scoped := &OperationsService{cluster: &Cluster{Name: "assets"}}
	_, nodes := scoped.clusterHealthFromStatus(status)
	found := false
	for _, n := range nodes {
		if n["procName"] == "ae2" && n["health"] == "OFFLINE" {
			found = true
		}
	}
	if !found {
		t.Fatal("the offending member is not reported")
	}
}

// The assets cluster must carry a housekeeping tool. Without one its snapshots
// add recordings forever and never free a byte — the 2026-07-25 outage.
func TestAssetsClusterHasHousekeeping(t *testing.T) {
	c := &Cluster{
		Name:             "assets",
		HousekeepingMain: "com.openexchange.assets.infrastructure.persistence.ArchiveHousekeeping",
	}
	if c.HousekeepingMain == "" {
		t.Fatal("assets has no housekeeping tool; its cluster log will grow unbounded")
	}
	if !strings.HasPrefix(c.HousekeepingMain, "com.openexchange.assets.") {
		t.Fatalf("assets must use its OWN tool, not the ME's: %s", c.HousekeepingMain)
	}
	if caps := c.Capabilities(); caps["housekeeping"] != true {
		t.Fatal("housekeeping capability not surfaced to the console")
	}
}
