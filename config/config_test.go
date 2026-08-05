// SPDX-License-Identifier: Apache-2.0
package config

import "testing"

// TestAssetsStateDirDefault covers the AssetsStateDir env-var-backed field
// added for the Assets Engine (money ledger) state-on-disk work: unset
// ASSETS_STATE_DIR must fall back to the historical tmpfs path byte-for-byte
// (so behavior is unchanged for every existing deployment), and setting it
// must be honored verbatim.
func TestAssetsStateDirDefault(t *testing.T) {
	t.Setenv("ASSETS_STATE_DIR", "")
	cfg := Load()
	if got, want := cfg.AssetsStateDir, "/dev/shm/aeron-assets"; got != want {
		t.Fatalf("unset ASSETS_STATE_DIR: got %q, want %q", got, want)
	}
}

func TestAssetsStateDirOverride(t *testing.T) {
	t.Setenv("ASSETS_STATE_DIR", "/mnt/nvme0/aeron-assets")
	cfg := Load()
	if got, want := cfg.AssetsStateDir, "/mnt/nvme0/aeron-assets"; got != want {
		t.Fatalf("ASSETS_STATE_DIR override: got %q, want %q", got, want)
	}
}
