// SPDX-License-Identifier: Apache-2.0
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// desiredStateFile persists the operator's INTENT per service ("running" |
// "stopped") next to the admin binary, alongside active-profile.json. It is
// written whenever a service is explicitly started or stopped (Start/Stop/
// StartAll/StopAll) and read once at boot so the gateway can reconcile the
// stack to the operator's last intent instead of coming up idle.
//
// The distinction that makes this correct: a crash or the rapid-restart
// DISARM does NOT change desired state. A service the operator stopped stays
// stopped across a reboot; a service that SHOULD be running is brought back —
// even if it had crashed — so an outage self-heals but a deliberate stop is
// respected. A missing file (fresh install) means "no intent recorded yet":
// the box stays idle until the operator starts something the first time.
const desiredStateFile = "desired-state.json"

// DesiredRunning / DesiredStopped are the only two intents.
const (
	DesiredRunning = "running"
	DesiredStopped = "stopped"
)

type persistedDesiredState struct {
	Services  map[string]string `json:"services"`
	UpdatedAt string            `json:"updatedAt,omitempty"`
}

// ReadDesiredState returns the persisted per-service intent map. A missing or
// unreadable file yields an empty (non-nil) map — never an error — so callers
// can treat "absent" as "no intent recorded".
func ReadDesiredState(adminDir string) map[string]string {
	out := map[string]string{}
	data, err := os.ReadFile(filepath.Join(adminDir, desiredStateFile))
	if err != nil {
		return out
	}
	var p persistedDesiredState
	if err := json.Unmarshal(data, &p); err != nil {
		return out
	}
	for name, state := range p.Services {
		if state == DesiredRunning || state == DesiredStopped {
			out[name] = state
		}
	}
	return out
}

// PersistDesiredState atomically writes the intent map (temp file + rename,
// like the other admin state files) so a crash mid-write never leaves a
// half-written file that would misdirect the next boot's reconcile.
func PersistDesiredState(adminDir string, states map[string]string, now time.Time) error {
	path := filepath.Join(adminDir, desiredStateFile)
	data, err := json.MarshalIndent(persistedDesiredState{
		Services:  states,
		UpdatedAt: now.UTC().Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
