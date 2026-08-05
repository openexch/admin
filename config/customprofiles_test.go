// SPDX-License-Identifier: Apache-2.0
package config

import (
	"strings"
	"testing"
	"time"
)

func testProfilesConfig(t *testing.T) *Config {
	t.Helper()
	profiles, err := LoadProfiles()
	if err != nil {
		t.Fatalf("LoadProfiles: %v", err)
	}
	c := &Config{Profiles: profiles, customNames: map[string]bool{}}
	c.SetActive("demo", profiles["demo"])
	return c
}

func TestCustomProfilesPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	custom := map[string]Profile{"demo-lite": {
		Description: "test", NodeHeapMB: 512, OmsHeapMB: 512, MarketHeapMB: 512, BackupHeapMB: 512,
		IdleMode: "backoff", DriverProfile: "dev", DriverMode: "external", BookCapacity: 1024,
		LogTermLength: "16m", MinMemMB: 512, SimGlobalOps: 5, Governor: "schedutil", THP: "madvise", Pinning: "none",
	}}
	if err := PersistCustomProfiles(dir, custom, time.Now()); err != nil {
		t.Fatalf("persist: %v", err)
	}
	got := ReadCustomProfiles(dir)
	if len(got) != 1 || got["demo-lite"].BookCapacity != 1024 {
		t.Fatalf("round trip lost data: %+v", got)
	}

	// Absent file → nil, no error path.
	if got := ReadCustomProfiles(t.TempDir()); got != nil {
		t.Fatalf("missing file should read nil, got %+v", got)
	}
}

func TestReadCustomProfilesSkipsInvalid(t *testing.T) {
	dir := t.TempDir()
	bad := map[string]Profile{"broken": {NodeHeapMB: -1}}
	if err := PersistCustomProfiles(dir, bad, time.Now()); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if got := ReadCustomProfiles(dir); len(got) != 0 {
		t.Fatalf("invalid entries must be skipped, got %+v", got)
	}
}

func TestCustomProfileAccessors(t *testing.T) {
	c := testProfilesConfig(t)
	custom := c.Profiles["demo"] // start from the preset's values
	custom.BookCapacity = 12345

	c.UpsertCustomProfile("demo-lite", custom)
	if c.IsBuiltin("demo-lite") {
		t.Fatal("saved custom must not report builtin")
	}
	if !c.IsBuiltin("demo") {
		t.Fatal("preset must report builtin")
	}
	if p, ok := c.ProfileByName("demo-lite"); !ok || p.BookCapacity != 12345 {
		t.Fatalf("custom not retrievable: %v %v", p, ok)
	}

	// Editing the ACTIVE profile moves the live bundle with it.
	c.SetActive("demo-lite", custom)
	edited := custom
	edited.NodeHeapMB = 640
	c.UpsertCustomProfile("demo-lite", edited)
	if _, live := c.Active(); live.NodeHeapMB != 640 {
		t.Fatalf("editing the active custom must update the live bundle, got %d", live.NodeHeapMB)
	}

	// Deletion guards: presets and unknowns refuse.
	if _, err := c.DeleteCustomProfile("demo"); err == nil || !strings.Contains(err.Error(), "built-in") {
		t.Fatalf("preset delete must refuse: %v", err)
	}
	if _, err := c.DeleteCustomProfile("nope"); err == nil {
		t.Fatal("unknown delete must refuse")
	}
	if customs, err := c.DeleteCustomProfile("demo-lite"); err != nil || len(customs) != 0 {
		t.Fatalf("custom delete should succeed and empty the set: %v %v", customs, err)
	}
	if _, ok := c.ProfileByName("demo-lite"); ok {
		t.Fatal("deleted custom must be gone")
	}
}

func TestValidateStrict(t *testing.T) {
	profiles, _ := LoadProfiles()
	base := profiles["light"]

	// Every embedded preset must pass strict validation at the default topology
	// (3 nodes) on a 32GB box — otherwise apply would refuse our own presets.
	for name, p := range profiles {
		if err := p.ValidateStrict(3, 31853); err != nil {
			t.Errorf("preset %s must pass strict validation: %v", name, err)
		}
	}

	bad := base
	bad.LogTermLength = "17m"
	if err := bad.ValidateStrict(3, 31853); err == nil || !strings.Contains(err.Error(), "logTermLength") {
		t.Fatalf("invalid term length must fail: %v", err)
	}

	big := base
	big.NodeHeapMB = 8192
	big.MinMemMB = 16384
	if err := big.ValidateStrict(3, 31853); err == nil || !strings.Contains(err.Error(), "RAM") {
		t.Fatalf("heaps+floor beyond RAM must fail: %v", err)
	}

	pinned := profiles["demo"] // pinning: dedicated
	if err := pinned.ValidateStrict(5, 31853); err == nil || !strings.Contains(err.Error(), "pinning") {
		t.Fatalf("dedicated pinning at 5 nodes must fail: %v", err)
	}
	if err := pinned.ValidateStrict(3, 0); err != nil {
		t.Fatalf("unknown RAM (0) skips the ceiling check: %v", err)
	}
}
