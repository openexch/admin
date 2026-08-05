// SPDX-License-Identifier: Apache-2.0
package config

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Custom (operator-defined) runtime profiles. The 5 embedded presets are
// immutable; customs live in custom-profiles.json next to the admin binary,
// merged over the presets at boot, and edited through the profiles CRUD API.
// A custom profile is selectable/appliable exactly like a preset — including
// being the persisted active profile across restarts.

const customProfilesFile = "custom-profiles.json"

type persistedCustomProfiles struct {
	Profiles  map[string]Profile `json:"profiles"`
	UpdatedAt string             `json:"updatedAt"`
}

// ReadCustomProfiles loads the operator's custom profiles. Missing file or
// unparseable content is not an error — the stack always boots on the presets.
// Entries failing per-field validation are skipped with a warning rather than
// failing boot (the operator fixes them through the API).
func ReadCustomProfiles(adminDir string) map[string]Profile {
	data, err := os.ReadFile(filepath.Join(adminDir, customProfilesFile))
	if err != nil {
		return nil
	}
	var p persistedCustomProfiles
	if err := json.Unmarshal(data, &p); err != nil {
		slog.Warn("custom-profiles.json unreadable, ignoring", "err", err)
		return nil
	}
	out := make(map[string]Profile, len(p.Profiles))
	for name, prof := range p.Profiles {
		if err := prof.validate(); err != nil {
			slog.Warn("custom profile invalid, skipping", "profile", name, "err", err)
			continue
		}
		out[name] = prof
	}
	return out
}

// PersistCustomProfiles atomically writes the custom-profile set (temp file +
// rename, like the other admin state files).
func PersistCustomProfiles(adminDir string, profiles map[string]Profile, now time.Time) error {
	p := persistedCustomProfiles{Profiles: profiles, UpdatedAt: now.UTC().Format(time.RFC3339)}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(adminDir, customProfilesFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
