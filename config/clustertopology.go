// SPDX-License-Identifier: Apache-2.0
package config

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Cluster topology (node count per cluster) is deliberately NOT part of the
// runtime profile: changing it re-forms the cluster from genesis (Aeron 1.51
// has static membership — the member list is baked into every node's launch
// env), which destroys cluster state. Keeping it in its own store means
// applying a profile can never wipe data; topology changes go through their
// own explicitly-confirmed endpoint.
//
// The store mirrors active-profile.json: a tiny JSON file next to the admin
// binary, atomically replaced, tolerantly read (absent/garbage → descriptor
// defaults, i.e. match=3 / assets=1).

const topologyFile = "cluster-topology.json"

type persistedTopology struct {
	// Clusters maps cluster name → node count (e.g. {"match": 3, "assets": 1}).
	Clusters  map[string]int `json:"clusters"`
	UpdatedAt string         `json:"updatedAt"`
}

// ReadTopology loads the persisted per-cluster node counts. Missing file or
// unparseable content is not an error — the caller falls back to the
// descriptor defaults.
func ReadTopology(adminDir string) map[string]int {
	data, err := os.ReadFile(filepath.Join(adminDir, topologyFile))
	if err != nil {
		return nil
	}
	var p persistedTopology
	if err := json.Unmarshal(data, &p); err != nil {
		slog.Warn("cluster-topology.json unreadable, using descriptor defaults", "err", err)
		return nil
	}
	return p.Clusters
}

// PersistTopology atomically writes the per-cluster node counts (temp file +
// rename, like PersistActiveProfile) so a crash mid-write can never brick boot.
func PersistTopology(adminDir string, clusters map[string]int, now time.Time) error {
	p := persistedTopology{Clusters: clusters, UpdatedAt: now.UTC().Format(time.RFC3339)}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(adminDir, topologyFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
