// SPDX-License-Identifier: Apache-2.0
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	Port             string
	BindAddr         string // listen address; loopback by default (admin-gateway#11)
	AuthToken        string // bearer token for the admin API; empty = loopback-only dev mode
	ProjectDir       string
	AdminDir         string // this repo's checkout (admin-gateway source + live binary)
	OmsProjectDir    string
	JarPath          string // Cluster JAR (for ClusterTool operations)
	GatewayJar       string // Gateway JAR
	OmsJar           string // OMS uber JAR
	AssetsProjectDir string // Assets Engine repo checkout
	AssetsJar        string // Assets Engine cluster uber JAR (ae0 node)
	AssetsBridgeJar  string // Settlement bridge uber JAR (ME journal -> AE ingress; env ASSETS_BRIDGE_JAR)
	// BridgeJournalArchives is the settlement bridge's ME journal archive control
	// endpoint list (env BRIDGE_ME_JOURNAL_ARCHIVES), forwarded verbatim as the
	// bridge process's own env var of the same name.
	BridgeJournalArchives string
	LogDir                string
	// ClusterDir is the matching engine's cluster+archive state root (env
	// MATCH_STATE_DIR). Defaults to the historical tmpfs path
	// (/dev/shm/aeron-cluster) so behavior is unchanged when the env var is
	// unset. Same durability caveat as AssetsStateDir: tmpfs state does not
	// survive a reboot; all three replicas share this box.
	ClusterDir string
	// AssetsStateDir is the Assets Engine's (money ledger) cluster+archive
	// state root (env ASSETS_STATE_DIR). Defaults to the historical tmpfs
	// path (/dev/shm/aeron-assets) so behavior is unchanged when the env var
	// is unset. A power loss wipes a tmpfs-backed ledger; see the
	// ae-state-on-disk preflight check (services/preflight.go) for the
	// invariant that surfaces this before it bites in production.
	AssetsStateDir string

	// WatermarkRetention APPLIES the retention watermark to the purge
	// (env WATERMARK_RETENTION=true). Default false: the watermark is computed,
	// logged and reported either way, but until this is on the purge keeps its
	// old snapshot-only bound.
	//
	// Split from the calculation deliberately. The watermark replaces a boolean
	// whose two branches are "strand a member" and "grow without bound", so it
	// is a real improvement — but it changes what gets DELETED from a live money
	// ledger, and the direction that hurts is deleting what a member still
	// needed. Watching the computed numbers under real traffic costs nothing;
	// being wrong costs a reseed of the ledger.
	WatermarkRetention bool

	// SettlementJournalDir is the settlement-journal root (env SETTLEMENT_JOURNAL_DIR),
	// matching the cluster's SETTLEMENT_JOURNAL_DIR. Empty = the journal feature is off,
	// so /api/admin/journal-retention refuses with 409 rather than purging an unknown dir.
	// The per-node journal lives at <SettlementJournalDir>/node<N>.
	SettlementJournalDir string
	LogFormat            string // "json" (default) or "text" (ADMIN_LOG_FORMAT)
	SimBinary            string // market simulator binary (SIM_BINARY; openexch/tools market-sim)

	// Pre-flight invariant thresholds (services/preflight.go, #42/#43/#45).
	// Defaults sized for the 31G demo box: steady-state MemAvailable there is
	// ~7-8GB with the full stack up, and the #43 OOM hit during a node
	// restart's catchup transient.
	MinMemMB      int // block gated ops below this MemAvailable (ADMIN_MIN_MEM_MB)
	MinRootDiskGB int // block gated ops below this free space on / (ADMIN_MIN_ROOT_DISK_GB)
	MaxShmUsedPct int // block gated ops above this /dev/shm usage (ADMIN_MAX_SHM_USED_PCT)
	BuildNice     int // niceness for rebuild mvn/go/rsync (ADMIN_BUILD_NICE; 0 disables)

	// Runtime profile (profiles.go): the active tuning bundle and the full set.
	// ProfileName is chosen at boot (state file > STACK_PROFILE > demo); Profile
	// drives the service catalog (heaps/idle/pinning/etc.) and the preflight mem
	// gate. Profiles is immutable after Load. ProfileName/Profile/MinMemMB become
	// MUTABLE once POST /api/admin/profile lands (Phase 2), so every concurrent
	// reader must go through Active()/MinMem() — direct field reads are only safe
	// at boot, before the server serves. mu guards the mutable trio; the setter
	// is SetActive.
	mu          sync.RWMutex
	ProfileName string
	Profile     Profile
	Profiles    map[string]Profile

	// minMemOverride is the operator's ADMIN_MIN_MEM_MB escape hatch (nil when
	// unset). When present it pins MinMemMB across profile switches so a live
	// switch never silently drops the emergency floor an operator set by hand.
	minMemOverride *int

	// topology is the persisted per-cluster node-count map (cluster-topology.json,
	// clustertopology.go), guarded by mu like the profile trio. Absent entries
	// fall back to the descriptor constructor defaults (match=3, assets=1).
	topology map[string]int

	// customNames marks which entries of Profiles are operator-defined customs
	// (custom-profiles.json) vs immutable embedded presets. Guarded by mu:
	// the profiles CRUD mutates Profiles live, so every post-boot read of the
	// set goes through ProfileByName/ProfilesSnapshot.
	customNames map[string]bool
}

// ProfileByName returns one profile from the live set (presets + customs).
func (c *Config) ProfileByName(name string) (Profile, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.Profiles[name]
	return p, ok
}

// ProfilesSnapshot returns a copy of the live profile set.
func (c *Config) ProfilesSnapshot() map[string]Profile {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]Profile, len(c.Profiles))
	for k, v := range c.Profiles {
		out[k] = v
	}
	return out
}

// IsBuiltin reports whether name is one of the immutable embedded presets.
func (c *Config) IsBuiltin(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.Profiles[name]
	return exists && !c.customNames[name]
}

// UpsertCustomProfile creates or updates a custom profile and returns the
// customs snapshot for persistence. The caller has already validated the
// profile and rejected preset names.
func (c *Config) UpsertCustomProfile(name string, p Profile) map[string]Profile {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.customNames == nil {
		c.customNames = map[string]bool{}
	}
	c.Profiles[name] = p
	c.customNames[name] = true
	// If the edited profile is the ACTIVE one, the live bundle moves with it so
	// the next apply/diff reasons from what the operator just saved.
	if c.ProfileName == name {
		c.Profile = p
		if c.minMemOverride == nil {
			c.MinMemMB = p.MinMemMB
		}
	}
	return c.customsSnapshotLocked()
}

// DeleteCustomProfile removes a custom profile and returns the customs
// snapshot for persistence. Presets and unknown names are refused; the ACTIVE
// check is the caller's (it owns the error message).
func (c *Config) DeleteCustomProfile(name string) (map[string]Profile, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.Profiles[name]; !ok {
		return nil, fmt.Errorf("unknown profile %q", name)
	}
	if !c.customNames[name] {
		return nil, fmt.Errorf("%q is a built-in preset and cannot be deleted", name)
	}
	delete(c.Profiles, name)
	delete(c.customNames, name)
	return c.customsSnapshotLocked(), nil
}

// customsSnapshotLocked copies the custom subset; callers hold mu.
func (c *Config) customsSnapshotLocked() map[string]Profile {
	out := map[string]Profile{}
	for name := range c.customNames {
		if p, ok := c.Profiles[name]; ok {
			out[name] = p
		}
	}
	return out
}

// NodeCountFor returns the persisted node count for a cluster, or def when the
// topology store has no entry for it.
func (c *Config) NodeCountFor(name string, def int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if n, ok := c.topology[name]; ok && n > 0 {
		return n
	}
	return def
}

// SetNodeCount commits a topology change in memory and returns a snapshot of
// the full map for persistence. The caller persists via PersistTopology.
func (c *Config) SetNodeCount(name string, n int) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.topology == nil {
		c.topology = map[string]int{}
	}
	c.topology[name] = n
	snap := make(map[string]int, len(c.topology))
	for k, v := range c.topology {
		snap[k] = v
	}
	return snap
}

// Active returns the live active profile name and bundle under a read lock, so
// a concurrent POST /api/admin/profile switch is observed atomically. Boot-time
// code may read the fields directly (no writer exists yet); everything reachable
// from a request or the status poller must use this.
func (c *Config) Active() (string, Profile) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ProfileName, c.Profile
}

// ActiveName is the live active profile name (read-locked).
func (c *Config) ActiveName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ProfileName
}

// MinMem is the live preflight mem-available block threshold (MB), read-locked.
// Preflight and the status poller call this every cycle; a profile switch moves
// it atomically.
func (c *Config) MinMem() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.MinMemMB
}

// EffectiveMinMem returns the mem floor that WOULD be in force if prof were the
// active profile: the ADMIN_MIN_MEM_MB override if the operator set one, else
// prof's own MinMemMB. Used by the switch-up headroom check so it reasons about
// the post-switch floor, override included.
func (c *Config) EffectiveMinMem(prof Profile) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.minMemOverride != nil {
		return *c.minMemOverride
	}
	return prof.MinMemMB
}

// SetActive commits a profile switch in memory: ProfileName, Profile and the
// derived MinMemMB move together under the write lock. The ADMIN_MIN_MEM_MB
// override, when set, still wins over the profile's own floor.
func (c *Config) SetActive(name string, prof Profile) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ProfileName = name
	c.Profile = prof
	if c.minMemOverride != nil {
		c.MinMemMB = *c.minMemOverride
	} else {
		c.MinMemMB = prof.MinMemMB
	}
}

func Load() *Config {
	projectDir := os.Getenv("MATCH_PROJECT_DIR")
	if projectDir == "" {
		// Default to parent of admin-gateway
		exe, _ := os.Executable()
		projectDir = filepath.Dir(filepath.Dir(exe))
	}

	omsProjectDir := os.Getenv("OMS_PROJECT_DIR")
	if omsProjectDir == "" {
		// Default to the sibling oms directory. The repo has been named `oms` on
		// GitHub for a long time; only the checkout directory still said
		// `order-management`, and that mismatch is what this default encoded.
		//
		// A box provisioned before the rename still has order-management/ on
		// disk. It is NOT silently probed for here: a fallback that quietly
		// finds a different directory than the one named is how a stale path
		// turns into a wrong answer instead of a loud failure. Such a box must
		// set OMS_PROJECT_DIR explicitly, which `make install` now does.
		omsProjectDir = filepath.Join(filepath.Dir(projectDir), "oms")
	}

	// Assets Engine (money ledger) repo; sibling of match/order-management.
	assetsProjectDir := os.Getenv("ASSETS_PROJECT_DIR")
	if assetsProjectDir == "" {
		assetsProjectDir = filepath.Join(filepath.Dir(projectDir), "assets")
	}

	// admin-gateway lives in its OWN repo since the split; it is NOT under
	// MATCH_PROJECT_DIR anymore. Default to the running binary's directory,
	// which is the repo checkout in every deploy mode we have.
	adminDir := os.Getenv("ADMIN_PROJECT_DIR")
	if adminDir == "" {
		exe, _ := os.Executable()
		adminDir = filepath.Dir(exe)
	}

	homeDir, _ := os.UserHomeDir()

	// Resolve the active runtime profile. A malformed embedded default is a
	// build bug; fall back to a minimal safe demo so the admin still boots.
	profiles, err := LoadProfiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "profiles: %v; using built-in fallback\n", err)
		profiles = fallbackProfiles()
	}
	// Precedence for the boot profile: a persisted live switch (Phase 2's
	// active-profile.json) wins, then STACK_PROFILE, then demo — so a profile
	// chosen from the admin console survives an admin restart, while a fresh box
	// with no state file still honours the env default.
	// Merge the operator's custom profiles over the embedded presets BEFORE the
	// persisted-active check — a custom profile can be the active one across
	// restarts. Preset names win any collision (presets are immutable).
	customNames := map[string]bool{}
	for name, p := range ReadCustomProfiles(adminDir) {
		if _, exists := profiles[name]; exists {
			fmt.Fprintf(os.Stderr, "profiles: custom profile %q collides with a preset; ignoring the custom\n", name)
			continue
		}
		profiles[name] = p
		customNames[name] = true
	}

	profileName := ActiveProfileName(profiles)
	if persisted, ok := ReadPersistedProfile(adminDir); ok {
		if _, exists := profiles[persisted]; exists {
			profileName = persisted
		} else {
			fmt.Fprintf(os.Stderr, "profiles: persisted profile %q not in the set; using %q\n", persisted, profileName)
		}
	}
	profile := profiles[profileName]

	// The profile sets the mem-available block threshold; ADMIN_MIN_MEM_MB, when
	// explicitly set, remains an operator override (emergency escape hatch) that
	// also pins the floor across live profile switches (see SetActive).
	minMemMB := profile.MinMemMB
	var minMemOverride *int
	if v := os.Getenv("ADMIN_MIN_MEM_MB"); v != "" {
		minMemMB = getEnvIntOrDefault("ADMIN_MIN_MEM_MB", minMemMB)
		ov := minMemMB
		minMemOverride = &ov
	}

	return &Config{
		Port:             getEnvOrDefault("ADMIN_PORT", "8082"),
		BindAddr:         getEnvOrDefault("ADMIN_BIND", "127.0.0.1"),
		AuthToken:        loadAuthToken(),
		ProjectDir:       projectDir,
		AdminDir:         adminDir,
		OmsProjectDir:    omsProjectDir,
		JarPath:          filepath.Join(projectDir, "match-cluster/target/match-cluster.jar"),
		GatewayJar:       filepath.Join(projectDir, "match-gateway/target/match-gateway.jar"),
		OmsJar:           filepath.Join(omsProjectDir, "oms-app/target/oms-app.jar"),
		AssetsProjectDir: assetsProjectDir,
		AssetsJar:        filepath.Join(assetsProjectDir, "assets-cluster/target/assets-cluster.jar"),
		AssetsBridgeJar: getEnvOrDefault("ASSETS_BRIDGE_JAR",
			filepath.Join(assetsProjectDir, "assets-bridge/target/assets-bridge.jar")),
		BridgeJournalArchives: getEnvOrDefault("BRIDGE_ME_JOURNAL_ARCHIVES",
			"localhost:9010,localhost:9110,localhost:9210"),
		LogDir:               filepath.Join(homeDir, ".local/log/cluster"),
		ClusterDir:           getEnvOrDefault("MATCH_STATE_DIR", "/dev/shm/aeron-cluster"),
		AssetsStateDir:       getEnvOrDefault("ASSETS_STATE_DIR", "/dev/shm/aeron-assets"),
		WatermarkRetention:   os.Getenv("WATERMARK_RETENTION") == "true",
		SettlementJournalDir: os.Getenv("SETTLEMENT_JOURNAL_DIR"),
		LogFormat:            getEnvOrDefault("ADMIN_LOG_FORMAT", "json"),
		// tools is a sibling repo of match/order-management; build with
		// `cd tools/market-sim && go build -o market-sim .`
		SimBinary: getEnvOrDefault("SIM_BINARY",
			filepath.Join(filepath.Dir(projectDir), "tools/market-sim/market-sim")),
		MinMemMB:       minMemMB,
		minMemOverride: minMemOverride,
		MinRootDiskGB:  getEnvIntOrDefault("ADMIN_MIN_ROOT_DISK_GB", 5),
		MaxShmUsedPct:  getEnvIntOrDefault("ADMIN_MAX_SHM_USED_PCT", 90),
		BuildNice:      getEnvIntOrDefault("ADMIN_BUILD_NICE", 10),
		ProfileName:    profileName,
		Profile:        profile,
		Profiles:       profiles,
		customNames:    customNames,
		topology:       ReadTopology(adminDir),
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvIntOrDefault(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return defaultVal
}

// loadAuthToken reads the admin API bearer token from ADMIN_AUTH_TOKEN, or
// from the file named by ADMIN_AUTH_TOKEN_FILE (surrounding whitespace trimmed).
func loadAuthToken() string {
	return loadToken("ADMIN_AUTH_TOKEN")
}

// loadToken reads a token from <envKey> or the file named by <envKey>_FILE
// (whitespace trimmed). Unreadable files fail closed to an empty token: main
// refuses insecure bind combinations without one.
func loadToken(envKey string) string {
	if tok := os.Getenv(envKey); tok != "" {
		return tok
	}
	if file := os.Getenv(envKey + "_FILE"); file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(data))
	}
	return ""
}
