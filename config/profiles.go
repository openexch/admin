// SPDX-License-Identifier: Apache-2.0
package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Runtime profiles (light/dev/demo/performance/ultra): one named bundle that
// drives every managed service's JVM heap, idle strategy, CPU pinning, book
// capacity, log-term size, sim load, OS governor/THP and the preflight mem
// gate. The values live in profiles.json (embedded default below; override the
// whole file at runtime with ADMIN_PROFILES_FILE to retune without a rebuild).
// The active profile is selected by STACK_PROFILE (default "demo").

//go:embed profiles.json
var embeddedProfilesJSON []byte

// Profile is one runtime tuning bundle. Every field maps to a concrete knob;
// see profiles.json for the per-profile values and process_manager.go for how
// they are applied to each service's command/env.
type Profile struct {
	Description string `json:"description"`

	// JVM heap (MB) per role; -Xmx == -Xms. PreTouch toggles
	// -XX:+AlwaysPreTouch (commit the full heap at boot vs grow on demand).
	NodeHeapMB   int  `json:"nodeHeapMB"`
	OmsHeapMB    int  `json:"omsHeapMB"`
	MarketHeapMB int  `json:"marketHeapMB"`
	BackupHeapMB int  `json:"backupHeapMB"`
	PreTouch     bool `json:"preTouch"`

	IdleMode      string `json:"idleMode"`      // busy_spin | backoff | sleep (TRANSPORT_IDLE_MODE)
	DriverProfile string `json:"driverProfile"` // dev | prod (launch-driver.sh --profile)
	DriverMode    string `json:"driverMode"`    // embedded | external
	Threading     string `json:"threading"`     // ""(engine default=dedicated) | dedicated | shared | shared_network (TRANSPORT_DRIVER_THREADING)

	BookCapacity  int    `json:"bookCapacity"`  // MATCH_ENGINE_BOOK_CAPACITY (mem ∝)
	LogTermLength string `json:"logTermLength"` // TRANSPORT_LOG_TERM_LENGTH ("16m".."64m")

	MinMemMB     int `json:"minMemMB"`     // preflight mem-available block threshold
	SimGlobalOps int `json:"simGlobalOps"` // sim -global-ops; 0 = sim disabled

	Governor string `json:"governor"` // performance | schedutil | powersave
	THP      string `json:"thp"`      // never | madvise | always

	Pinning string `json:"pinning"` // dedicated | none (taskset core layout)
}

type profileFile struct {
	Profiles map[string]Profile `json:"profiles"`
}

// fallbackProfiles is the last-resort set used only if the embedded JSON fails
// to parse (a build bug). Mirrors the demo profile so the admin boots sanely.
func fallbackProfiles() map[string]Profile {
	return map[string]Profile{
		"demo": {
			Description: "built-in fallback (demo)",
			NodeHeapMB:  1536, OmsHeapMB: 1024, MarketHeapMB: 1024, BackupHeapMB: 768,
			PreTouch:      true,
			IdleMode:      "backoff",
			DriverProfile: "dev",
			DriverMode:    "external",
			BookCapacity:  65536,
			LogTermLength: "32m",
			MinMemMB:      3072,
			SimGlobalOps:  60,
			Governor:      "performance",
			THP:           "never",
			Pinning:       "dedicated",
		},
	}
}

// LoadProfiles returns the profile set: the embedded default, or the file named
// by ADMIN_PROFILES_FILE if it is set and readable (a bad override falls back to
// the embedded default with a warning rather than bricking the admin).
func LoadProfiles() (map[string]Profile, error) {
	raw := embeddedProfilesJSON
	if path := os.Getenv("ADMIN_PROFILES_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "profiles: cannot read ADMIN_PROFILES_FILE %q (%v); using embedded defaults\n", path, err)
		} else {
			raw = data
		}
	}
	var pf profileFile
	if err := json.Unmarshal(raw, &pf); err != nil {
		return nil, fmt.Errorf("parse profiles json: %w", err)
	}
	if len(pf.Profiles) == 0 {
		return nil, fmt.Errorf("profiles json has no profiles")
	}
	for name, p := range pf.Profiles {
		if err := p.validate(); err != nil {
			return nil, fmt.Errorf("profile %q: %w", name, err)
		}
	}
	return pf.Profiles, nil
}

func (p Profile) validate() error {
	if p.NodeHeapMB <= 0 || p.OmsHeapMB <= 0 || p.MarketHeapMB <= 0 || p.BackupHeapMB <= 0 {
		return fmt.Errorf("heap sizes must be positive")
	}
	if p.BookCapacity <= 0 {
		return fmt.Errorf("bookCapacity must be positive")
	}
	if p.MinMemMB <= 0 {
		return fmt.Errorf("minMemMB must be positive")
	}
	if p.SimGlobalOps < 0 {
		return fmt.Errorf("simGlobalOps must be >= 0")
	}
	if err := oneOf("idleMode", p.IdleMode, "busy_spin", "backoff", "sleep"); err != nil {
		return err
	}
	if err := oneOf("driverProfile", p.DriverProfile, "dev", "prod"); err != nil {
		return err
	}
	if err := oneOf("driverMode", p.DriverMode, "embedded", "external"); err != nil {
		return err
	}
	// Threading is optional: "" leaves the engine's own default (dedicated).
	if err := oneOf("threading", p.Threading, "", "dedicated", "shared", "shared_network"); err != nil {
		return err
	}
	if err := oneOf("governor", p.Governor, "performance", "schedutil", "powersave", "ondemand"); err != nil {
		return err
	}
	if err := oneOf("thp", p.THP, "never", "madvise", "always"); err != nil {
		return err
	}
	if err := oneOf("pinning", p.Pinning, "dedicated", "none"); err != nil {
		return err
	}
	return nil
}

// validTermLengths are Aeron's accepted log-term sizes (power-of-two);
// LogTermLength is otherwise a free string passed straight to the env, so a
// typo would only surface as a node boot failure.
var validTermLengths = map[string]bool{"16m": true, "32m": true, "64m": true, "128m": true}

// ValidateStrict runs the cross-field invariants a CUSTOM profile must satisfy
// on top of validate()'s per-field checks. nodeCount is the matching engine's
// LIVE topology (the heaviest heap multiplier) and memTotalMB the box's
// physical RAM: a profile that cannot coexist with its own mem floor on this
// box is refused at save/apply time instead of OOMing the stack later.
func (p Profile) ValidateStrict(nodeCount int, memTotalMB int64) error {
	if err := p.validate(); err != nil {
		return err
	}
	if !validTermLengths[p.LogTermLength] {
		return fmt.Errorf("logTermLength %q must be one of 16m, 32m, 64m, 128m", p.LogTermLength)
	}
	totalHeap := int64(p.NodeHeapMB*nodeCount + p.OmsHeapMB + p.MarketHeapMB + p.BackupHeapMB)
	if memTotalMB > 0 && totalHeap+int64(p.MinMemMB) > memTotalMB {
		return fmt.Errorf("total JVM heap %dMB (%d nodes) + minMemMB floor %dMB exceeds the box's %dMB RAM",
			totalHeap, nodeCount, p.MinMemMB, memTotalMB)
	}
	if p.Pinning == "dedicated" && nodeCount > 3 {
		return fmt.Errorf("pinning:dedicated only has core quads for 3 nodes; the cluster runs %d — use pinning:none", nodeCount)
	}
	return nil
}

func oneOf(field, val string, allowed ...string) error {
	for _, a := range allowed {
		if val == a {
			return nil
		}
	}
	return fmt.Errorf("%s %q must be one of %v", field, val, allowed)
}

// ActiveProfileName returns the requested profile name (STACK_PROFILE) if it
// exists in the set, else "demo" if present, else any profile — with a warning
// so a typo'd STACK_PROFILE degrades to a safe default instead of failing boot.
func ActiveProfileName(profiles map[string]Profile) string {
	want := os.Getenv("STACK_PROFILE")
	if want == "" {
		want = "demo"
	}
	if _, ok := profiles[want]; ok {
		return want
	}
	fmt.Fprintf(os.Stderr, "profiles: STACK_PROFILE %q not found; falling back to demo\n", want)
	if _, ok := profiles["demo"]; ok {
		return "demo"
	}
	names := ProfileNames(profiles)
	return names[0]
}

// ProfileNames returns the profile names in a stable, presentation-friendly
// order (light→dev→demo→performance→ultra first, then any extras alphabetically).
func ProfileNames(profiles map[string]Profile) []string {
	rank := map[string]int{"light": 0, "dev": 1, "demo": 2, "performance": 3, "ultra": 4}
	names := make([]string, 0, len(profiles))
	for n := range profiles {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		ri, oki := rank[names[i]]
		rj, okj := rank[names[j]]
		if oki && okj {
			return ri < rj
		}
		if oki != okj {
			return oki
		}
		return names[i] < names[j]
	})
	return names
}
