// SPDX-License-Identifier: Apache-2.0
package config

import (
	"os"
	"testing"
)

func TestLoadProfilesEmbedded(t *testing.T) {
	profiles, err := LoadProfiles()
	if err != nil {
		t.Fatalf("LoadProfiles: %v", err)
	}
	for _, name := range []string{"light", "dev", "demo", "performance", "ultra"} {
		if _, ok := profiles[name]; !ok {
			t.Errorf("missing shipped profile %q", name)
		}
	}
	// Every embedded profile must pass validation.
	for name, p := range profiles {
		if err := p.validate(); err != nil {
			t.Errorf("profile %q invalid: %v", name, err)
		}
	}
	// Memory story: light commits far less than demo, demo far less than ultra.
	if !(profiles["light"].NodeHeapMB < profiles["demo"].NodeHeapMB &&
		profiles["demo"].NodeHeapMB < profiles["ultra"].NodeHeapMB) {
		t.Errorf("node heap should grow light<demo<ultra: %d/%d/%d",
			profiles["light"].NodeHeapMB, profiles["demo"].NodeHeapMB, profiles["ultra"].NodeHeapMB)
	}
}

func TestActiveProfileName(t *testing.T) {
	profiles, err := LoadProfiles()
	if err != nil {
		t.Fatalf("LoadProfiles: %v", err)
	}

	t.Setenv("STACK_PROFILE", "")
	if got := ActiveProfileName(profiles); got != "demo" {
		t.Errorf("default active profile = %q, want demo", got)
	}

	t.Setenv("STACK_PROFILE", "light")
	if got := ActiveProfileName(profiles); got != "light" {
		t.Errorf("STACK_PROFILE=light => %q, want light", got)
	}

	// A typo falls back to demo rather than failing.
	t.Setenv("STACK_PROFILE", "nonesuch")
	if got := ActiveProfileName(profiles); got != "demo" {
		t.Errorf("unknown STACK_PROFILE => %q, want demo fallback", got)
	}
}

func TestProfileValidateRejectsBad(t *testing.T) {
	bad := Profile{
		NodeHeapMB: 512, OmsHeapMB: 512, MarketHeapMB: 512, BackupHeapMB: 512,
		BookCapacity: 1024, MinMemMB: 512, SimGlobalOps: 0,
		IdleMode: "spin-forever", DriverProfile: "dev", DriverMode: "external",
		Governor: "performance", THP: "never", Pinning: "dedicated",
	}
	if err := bad.validate(); err == nil {
		t.Error("expected invalid idleMode to be rejected")
	}
}

// Guard against a stray ADMIN_PROFILES_FILE in the test env changing results.
func TestMain(m *testing.M) {
	os.Unsetenv("ADMIN_PROFILES_FILE")
	os.Exit(m.Run())
}
