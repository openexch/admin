// SPDX-License-Identifier: Apache-2.0
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// activeProfileFile persists the operator's last live profile choice (POST
// /api/admin/profile) next to the admin binary, mirroring rebuild-pending.json.
// It is the top precedence at boot (see Load): a profile chosen from the console
// survives an admin restart. A missing or unreadable file is not an error — the
// box simply falls back to STACK_PROFILE / demo.
const activeProfileFile = "active-profile.json"

type persistedProfile struct {
	Profile   string `json:"profile"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// ReadPersistedProfile returns the persisted profile name and whether one was
// found. Caller validates the name against the loaded set (a stale name for a
// profile that no longer exists is ignored).
func ReadPersistedProfile(adminDir string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(adminDir, activeProfileFile))
	if err != nil {
		return "", false
	}
	var p persistedProfile
	if err := json.Unmarshal(data, &p); err != nil {
		return "", false
	}
	name := strings.TrimSpace(p.Profile)
	if name == "" {
		return "", false
	}
	return name, true
}

// PersistActiveProfile records name as the active profile via a temp-file +
// atomic rename, so a crash mid-write never leaves a half-written file that
// would brick the next boot's profile selection. now is passed in (the caller
// stamps it) to keep this dependency-free and testable.
func PersistActiveProfile(adminDir, name string, now time.Time) error {
	path := filepath.Join(adminDir, activeProfileFile)
	data, err := json.MarshalIndent(persistedProfile{
		Profile:   name,
		UpdatedAt: now.UTC().Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
