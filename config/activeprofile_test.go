// SPDX-License-Identifier: Apache-2.0
package config

import (
	"testing"
	"time"
)

func TestReadPersistedProfileMissing(t *testing.T) {
	if name, ok := ReadPersistedProfile(t.TempDir()); ok {
		t.Fatalf("no state file should read not-ok, got %q", name)
	}
}

func TestPersistAndReadActiveProfileRoundtrip(t *testing.T) {
	dir := t.TempDir()
	if err := PersistActiveProfile(dir, "performance", time.Unix(0, 0)); err != nil {
		t.Fatalf("persist: %v", err)
	}
	name, ok := ReadPersistedProfile(dir)
	if !ok || name != "performance" {
		t.Fatalf("roundtrip: got (%q,%v), want (performance,true)", name, ok)
	}
	// Overwrite must win (atomic replace, not append).
	if err := PersistActiveProfile(dir, "dev", time.Unix(0, 0)); err != nil {
		t.Fatalf("persist 2: %v", err)
	}
	if name, _ := ReadPersistedProfile(dir); name != "dev" {
		t.Fatalf("overwrite: got %q, want dev", name)
	}
}

func TestSetActiveMovesTrioAndHonoursOverride(t *testing.T) {
	profiles, err := LoadProfiles()
	if err != nil {
		t.Fatalf("LoadProfiles: %v", err)
	}
	demo, perf := profiles["demo"], profiles["performance"]

	// No override: MinMem tracks the profile floor.
	c := &Config{Profiles: profiles}
	c.SetActive("demo", demo)
	if n, p := c.Active(); n != "demo" || p.NodeHeapMB != demo.NodeHeapMB {
		t.Fatalf("Active after SetActive(demo) = (%q, heap %d)", n, p.NodeHeapMB)
	}
	if c.MinMem() != demo.MinMemMB {
		t.Fatalf("MinMem = %d, want demo floor %d", c.MinMem(), demo.MinMemMB)
	}
	c.SetActive("performance", perf)
	if c.MinMem() != perf.MinMemMB {
		t.Fatalf("MinMem after switch = %d, want perf floor %d", c.MinMem(), perf.MinMemMB)
	}

	// EffectiveMinMem without an override tracks the *target* profile's floor.
	if c.EffectiveMinMem(perf) != perf.MinMemMB {
		t.Fatalf("EffectiveMinMem(perf) = %d, want %d", c.EffectiveMinMem(perf), perf.MinMemMB)
	}

	// Operator override pins the floor across switches AND is what EffectiveMinMem
	// reports for any prospective target.
	override := 9999
	co := &Config{Profiles: profiles, minMemOverride: &override}
	co.SetActive("demo", demo)
	if co.MinMem() != override {
		t.Fatalf("override MinMem after demo = %d, want %d", co.MinMem(), override)
	}
	co.SetActive("performance", perf)
	if co.MinMem() != override {
		t.Fatalf("override MinMem must survive a switch, got %d want %d", co.MinMem(), override)
	}
	if co.EffectiveMinMem(demo) != override {
		t.Fatalf("EffectiveMinMem with override = %d, want %d", co.EffectiveMinMem(demo), override)
	}
}
