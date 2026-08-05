// SPDX-License-Identifier: Apache-2.0
package services

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/logging"
)

// ServiceRole defines the category of a managed service
// Core process types live in the agent package (the gateway↔agent contract,
// docs/AGENT-ARCHITECTURE.md); aliases keep the services API and JSON shape
// byte-identical.
type ServiceRole = agent.ServiceRole

const (
	RoleClusterNode = agent.RoleClusterNode
	RoleGateway     = agent.RoleGateway
	RoleInfra       = agent.RoleInfra
)

// ServiceDef defines how to start and manage a process
type ServiceDef struct {
	Name        string            `json:"name"`
	Display     string            `json:"display"`
	Role        ServiceRole       `json:"role"`
	Port        int               `json:"port,omitempty"`
	ExtraPorts  []int             `json:"-"` // additional ports to check (e.g. Aeron UDP egress)
	Command     []string          `json:"-"` // command + args
	Env         map[string]string `json:"-"` // extra environment variables
	WorkDir     string            `json:"-"` // working directory
	PreStart    [][]string        `json:"-"` // pre-start commands (run sequentially)
	DependsOn   []string          `json:"dependsOn,omitempty"`
	StartOrder  int               `json:"-"`
	AutoRestart bool              `json:"-"` // restart on crash
	RestartSec  int               `json:"-"` // seconds between restart attempts
	StopTimeout int               `json:"-"` // seconds to wait for graceful stop before SIGKILL
	WaitForFile string            `json:"-"` // block start until this file exists (e.g. media driver cnc.dat)
	// Services to force-stop when this one crashes and to start again after it restarts.
	// Used for media driver → node coupling: a node cannot outlive its external driver.
	RestartCascades []string `json:"-"`
	// Service that must be running AND stable before this one may start (see
	// waitForGate). Used for driver → node gating: starting a node against an
	// absent or flapping driver lets it write a partial archive and die
	// mid-write — the 2026-07-03 corruption engine (#17, match#35, match#48).
	GatedBy string `json:"-"`
	// External media driver aeron.dir this service owns (driver0-2 only).
	// Drives the #42 orphan protections: start refuses while a live untracked
	// driver holds <DriverDir>.pid, and force-stop kills that pid too.
	DriverDir string `json:"-"`
	// Launch artifact (jar/binary) this service execs. Start refuses when it
	// is missing (#45): relaunching into a vanished artifact exits instantly,
	// burns the crash-loop cap in seconds and disarms auto-restart.
	Artifact string `json:"-"`
}

// Restart-loop cap + driver gate tuning (#17). Vars, not consts, so tests can
// shrink the timings; production code never mutates them.
var (
	rapidCrashMax    = 5 // crashes within rapidCrashWindow before auto-restart disarms
	rapidCrashWindow = 2 * time.Minute
	gateStableFor    = 5 * time.Second  // gating service must be up this long (outlives launcher validation crashes)
	gateTimeout      = 25 * time.Second // total wait for the gate to become stable
	startStagger     = 2 * time.Second  // pause between StartAll service launches
)

// ProcessInfo is the live state of a managed service (defined in agent).
type ProcessInfo = agent.ProcessInfo

// managedProcess tracks a running process
type managedProcess struct {
	mu           sync.Mutex
	cmd          *exec.Cmd
	pid          int
	running      bool
	starting     bool // true while a start is in progress (prevents auto-restart race)
	startedAt    time.Time
	restartCount int
	status       string // "running", "stopped", "starting", "stopping", "failed", "crashed"
	stopChan     chan struct{}
	logFile      *os.File
	lastError    string      // why the last start failed / last crash happened (see ProcessInfo.LastError)
	crashTimes   []time.Time // recent crash timestamps; drives the rapid-restart cap (#17)
}

// ProcessManager directly manages processes (no systemd for managed services)
type ProcessManager struct {
	services []ServiceDef
	procs    map[string]*managedProcess // name → process state

	mu            sync.RWMutex
	pidDir        string
	logDir        string
	projectDir    string // backup-node state root (cleanStaleAeronState); empty = no backup cleanup
	matchStateDir string // ME state root for stale-mark cleanup (see ProcessManagerOptions)
	stopChan      chan struct{}
	log           *slog.Logger

	// adminDir is where desired-state.json lives (next to the admin binary);
	// empty disables desired-state persistence (tests, agentd M3 default).
	adminDir  string
	desiredMu sync.Mutex
	desired   map[string]string // name → config.DesiredRunning|DesiredStopped (operator intent)

	// LocalAgent surface (process_manager_agent.go)
	events       eventHub
	counters     *AeronCounters
	countersOnce sync.Once
}

// ProcessManagerOptions configures a ProcessManager independent of the
// gateway's env-derived config, so the same manager embeds in the standalone
// agentd (docs/AGENT-ARCHITECTURE.md Horizon B) and in tests with a fake
// catalog. The default constructor below builds today's single-box catalog
// and delegates here.
type ProcessManagerOptions struct {
	Services   []ServiceDef // the catalog; empty = manage nothing (agentd M3 default)
	LogDir     string       // default ~/.local/log/cluster
	PidDir     string       // default ~/.local/run/match
	ProjectDir string       // backup-node state root; empty disables backup cleanup
	// MatchStateDir is the matching-engine cluster+archive state root used by
	// stale-mark cleanup (cfg.ClusterDir / env MATCH_STATE_DIR). Empty falls
	// back to the historical tmpfs path.
	MatchStateDir string
	// AdminDir is where desired-state.json is persisted (next to the admin
	// binary). Empty disables desired-state tracking (tests, agentd default).
	AdminDir string
}

// NewProcessManagerWith builds a ProcessManager from explicit options,
// adopts already-running processes from PID files, and starts the metrics
// poller. Behavior is identical to NewProcessManager for the same catalog.
func NewProcessManagerWith(opts ProcessManagerOptions) *ProcessManager {
	logger := logging.Component("pm")
	homeDir, _ := os.UserHomeDir()
	logDir := opts.LogDir
	if logDir == "" {
		logDir = filepath.Join(homeDir, ".local/log/cluster")
	}
	pidDir := opts.PidDir
	if pidDir == "" {
		pidDir = filepath.Join(homeDir, ".local/run/match")
	}
	os.MkdirAll(logDir, 0755)
	os.MkdirAll(pidDir, 0755)

	msd := opts.MatchStateDir
	if msd == "" {
		msd = "/dev/shm/aeron-cluster"
	}
	pm := &ProcessManager{
		matchStateDir: msd,
		logDir:        logDir,
		pidDir:        pidDir,
		projectDir:    opts.ProjectDir,
		procs:         make(map[string]*managedProcess),
		services:      opts.Services,
		stopChan:      make(chan struct{}),
		log:           logger,
		adminDir:      opts.AdminDir,
		desired:       config.ReadDesiredState(opts.AdminDir),
	}

	// Initialize process state for each service
	for _, def := range pm.services {
		pm.procs[def.Name] = &managedProcess{
			status: "stopped",
		}
	}

	// Adopt any already-running processes (from PID files)
	pm.adoptExisting()

	// Start background metrics poller
	go pm.backgroundPoller()

	return pm
}

// NewProcessManager builds the single-box catalog (drivers, nodes, backup,
// gateways, sim) from the gateway config. The catalog construction is the
// only config-dependent part; everything else lives in NewProcessManagerWith.
// marketEdgeTokenFile is the bearer token the market gateway presents to the
// edge relay's /publish endpoint (edge/market-relay PUBLISH_TOKEN secret).
func marketEdgeTokenFile() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config/openexchange-market-edge.token")
}

func NewProcessManager(cfg *config.Config) *ProcessManager {
	_, prof := cfg.Active()
	return NewProcessManagerWith(ProcessManagerOptions{
		Services:      buildServiceCatalog(cfg, prof),
		ProjectDir:    cfg.ProjectDir,
		MatchStateDir: cfg.ClusterDir,
		AdminDir:      cfg.AdminDir,
	})
}

// buildServiceCatalog constructs the single-box service catalog (drivers,
// nodes, backup, gateways, sim) from cfg and the given runtime profile. Pure in
// (cfg, prof): the same inputs always yield the same []ServiceDef, so a live
// profile switch rebuilds it and diffs against the current catalog to roll only
// the services whose launch spec changed (services/operations_profile.go).
//
// The active runtime profile (config/profiles.go) drives heaps, idle strategy,
// pinning, book capacity, log-term, driver mode/profile and the sim load.
func buildServiceCatalog(cfg *config.Config, prof config.Profile) []ServiceDef {
	logger := logging.Component("pm")

	// javaFlags builds the JVM launch flags for a given heap (-Xmx==-Xms).
	// AlwaysPreTouch (commit the whole heap at boot) is profile-gated so the
	// light/dev profiles keep a small resident footprint instead of committing
	// the full heap up front.
	javaFlags := func(heapMB int) []string {
		flags := []string{
			"/usr/bin/java",
			"-XX:+UseZGC", "-XX:+ZGenerational",
			"-XX:+UnlockDiagnosticVMOptions", "-XX:GuaranteedSafepointInterval=0",
			"-XX:+UseNUMA", "-XX:+PerfDisableSharedMem",
			"-XX:+TieredCompilation", "-XX:TieredStopAtLevel=4",
			"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
			"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
			fmt.Sprintf("-Xmx%dm", heapMB), fmt.Sprintf("-Xms%dm", heapMB),
		}
		if prof.PreTouch {
			flags = append(flags, "-XX:+AlwaysPreTouch")
		}
		return flags
	}

	// pin prefixes a command with taskset for dedicated-core pinning; the "none"
	// profile (light) returns no prefix so scheduling stays with the OS and the
	// cores are free for other work (an IDE, a browser). An empty cpuSet (no
	// spare quad for this node index) also runs unpinned.
	pin := func(cpuSet string) []string {
		if prof.Pinning == "none" || cpuSet == "" {
			return nil
		}
		return []string{"/usr/bin/taskset", "-c", cpuSet}
	}

	// External media driver mode: one standalone driver process per node, engine
	// JVMs connect over shared-memory IPC (kernel-bypass-ready; see
	// match/docs/kernel-bypass.md). driverMode=embedded reverts to the in-JVM
	// ClusteredMediaDriver (no driver services). Driven by the profile.
	externalDriver := prof.DriverMode != "embedded"

	// dev driver profile: SHARED threads + backoff idles; prod: DEDICATED
	// busy-spin (+ SENDER/RECEIVER/CONDUCTOR_CORE pinning if set in the env).
	driverProfile := prof.DriverProfile

	driverDir := driverDirPath

	// The descriptors: node counts come from the topology store (via cfg), so the
	// whole catalog — member lists, service sets, dirs — is generated per count.
	matchCluster := NewMatchCluster(cfg)
	assetsCluster := NewAssetsCluster(cfg)

	// clusterAddresses is the Raft member list: N comma-separated hosts, one per
	// member (all loopback on this box, disambiguated by the per-node port
	// offset). Its LENGTH is what the JVM forms the cluster from — this literal
	// IS the topology.
	clusterAddresses := func(n int) string {
		return strings.TrimSuffix(strings.Repeat("127.0.0.1,", n), ",")
	}
	matchAddresses := clusterAddresses(matchCluster.NodeCount())

	driverCmd := func(nodeId int, cpuSet string) []string {
		cmd := pin(cpuSet)
		return append(cmd, "/usr/bin/bash",
			filepath.Join(cfg.ProjectDir, "deploy/media-driver/launch-driver.sh"),
			"--instance", matchCluster.NodeName(nodeId),
			"--profile", driverProfile)
	}

	nodeCmd := func(cpuSet string) []string {
		cmd := pin(cpuSet)
		cmd = append(cmd, javaFlags(prof.NodeHeapMB)...)
		return append(cmd, "-jar", "match-cluster/target/match-cluster.jar")
	}

	nodeEnv := func(nodeId int) map[string]string {
		env := map[string]string{
			"CLUSTER_ADDRESSES": matchAddresses,
			"CLUSTER_NODE":      strconv.Itoa(nodeId),
			"CLUSTER_PORT_BASE": strconv.Itoa(matchCluster.PortBase),
			"BASE_DIR":          matchCluster.NodeStateDir(nodeId),
			// Profile-driven engine/transport tuning; def.Env is layered after
			// os.Environ() so these override any inherited driver-profile.conf.
			"TRANSPORT_IDLE_MODE":        prof.IdleMode,
			"TRANSPORT_LOG_TERM_LENGTH":  prof.LogTermLength,
			"MATCH_ENGINE_BOOK_CAPACITY": strconv.Itoa(prof.BookCapacity),
		}
		// Media-driver / archive threading. Empty = leave the engine's own default
		// (DEDICATED); a constrained profile sets "shared" so the embedded driver runs
		// on a fraction of a core instead of busy-spinning 3 threads per node.
		if prof.Threading != "" {
			env["TRANSPORT_DRIVER_THREADING"] = prof.Threading
		}
		if externalDriver {
			env["TRANSPORT_DRIVER_MODE"] = "external"
			env["AERON_DIR"] = driverDir(nodeId)
		}
		// Settlement journal (dark by default): pass the switch + base dir through when the
		// admin has them; the node resolves its own <dir>/node<N> subdir. Enabling is a
		// one-line admin drop-in (SETTLEMENT_JOURNAL_ENABLED=true) + node roll.
		if cfg.SettlementJournalDir != "" {
			env["SETTLEMENT_JOURNAL_DIR"] = cfg.SettlementJournalDir
			if os.Getenv("SETTLEMENT_JOURNAL_ENABLED") == "true" {
				env["SETTLEMENT_JOURNAL_ENABLED"] = "true"
			}
		}
		return env
	}

	nodePreStart := func(nodeId int) [][]string {
		return [][]string{
			{"mkdir", "-p", matchCluster.NodeStateDir(nodeId)},
		}
	}

	// backup runs unpinned on the backup heap.
	gatewayCmd := func() []string {
		return javaFlags(prof.BackupHeapMB)
	}

	// Pinned client commands (market gateway, OMS) on cores disjoint from the
	// cluster's 0-11 so their poll threads don't compete with the cluster's idle
	// strategy. Heaps are per-role from the profile.
	pinnedGatewayCmd := func(cpuSet string) []string {
		return append(pin(cpuSet), javaFlags(prof.MarketHeapMB)...)
	}

	omsCmd := func(cpuSet string) []string {
		cmd := append(pin(cpuSet), javaFlags(prof.OmsHeapMB)...)
		return append(cmd, "-jar", cfg.OmsJar)
	}

	// Cluster node cores on the 13700K: dedicated 4-core quads exist for up to 3
	// nodes (0-3/4-7/8-11; OMS/market/sim own 12-23). Beyond 3 nodes there are no
	// spare quads — those nodes run unpinned (the pinning:none / light path; the
	// topology validator steers >3-node counts there).
	nodeCpuSet := func(i int) string {
		if i < 3 {
			return fmt.Sprintf("%d-%d", i*4, i*4+3)
		}
		return ""
	}
	pinnedNodeCmd := func(i int) []string {
		return nodeCmd(nodeCpuSet(i)) // empty set (node >= 3) runs unpinned via pin()
	}

	// nodeNames is the gateway dependency list: every cluster node, whatever the
	// count (gateways boot after the whole member set).
	nodeNames := func(c *Cluster) []string {
		names := make([]string, c.NodeCount())
		for i := range names {
			names[i] = c.NodeName(i)
		}
		return names
	}

	// The matching-engine cluster generates its own node + coupled media-driver
	// services from its descriptor: the driver is PART of the cluster and the
	// driver↔node linkage (DependsOn/GatedBy/WaitForFile/RestartCascades/DriverDir)
	// is derived, not hand-wired. Launch specifics stay profile-driven via the
	// existing nodeCmd/driverCmd/nodeEnv/nodePreStart closures. In embedded-driver
	// mode (light profile) no driver services are generated — the node runs its own.
	services := []ServiceDef{}
	services = append(services, matchCluster.NodeServiceDefs(
		externalDriver,
		pinnedNodeCmd,
		func(i int) []string { return driverCmd(i, nodeCpuSet(i)) },
		nodeEnv,
		nodePreStart,
	)...)
	services = append(services,
		[]ServiceDef{
			{
				Name: "backup", Display: "Backup Node", Role: RoleClusterNode,
				Command: append(gatewayCmd(), "-cp", "match-cluster/target/match-cluster.jar",
					"com.match.infrastructure.persistence.ClusterBackupApp"),
				// The env is LOAD-BEARING (match#36): without CLUSTER_ADDRESSES the app
				// defaults to a SINGLE consensus endpoint (node0's), and Aeron's
				// ClusterBackup wedges in an infinite nextCursor() loop the moment the
				// leader is any other node (single-endpoint PublicationGroup + exclusion).
				// BASE_DIR pins backup data to durable DISK, never tmpfs — it is the
				// power-loss recovery source (#9).
				Env: map[string]string{
					"CLUSTER_ADDRESSES":     matchAddresses,
					"BASE_DIR":              filepath.Join(cfg.ProjectDir, "backup"),
					"BACKUP_STALL_EXIT_SEC": "300",
				},
				WorkDir:     cfg.ProjectDir,
				PreStart:    [][]string{{"mkdir", "-p", filepath.Join(cfg.ProjectDir, "backup")}},
				DependsOn:   nodeNames(matchCluster),
				AutoRestart: true, RestartSec: 10, StopTimeout: 5,
				Artifact: cfg.JarPath,
			},
			{
				Name: "oms", Display: "Order Management", Role: RoleGateway, Port: 8080,
				ExtraPorts: []int{9093, 9090}, // Aeron UDP egress + gRPC
				Command:    omsCmd("12-15"),   // pinned: cores 12-15 (cluster uses 0-11)
				Env: map[string]string{
					"OMS_HTTP_PORT":     "8080",
					"OMS_GRPC_PORT":     "9090",
					"EGRESS_PORT":       "9093",
					"CLUSTER_ADDRESSES": matchAddresses,
					// Public demo auth (oms#72): self-registered users with opaque
					// tokens, scoped to their own data. The dev-token backdoor
					// stays for local infrastructure only (userId 1 + the sim
					// range 900000-900999, all self-scoped), so the market-sim
					// bots and the demo canary keep working unchanged.
					"OMS_AUTH_MODE": "demo",
					// CORS is default-deny since oms#37; the hosted demo UI is a
					// cross-origin browser client and needs an explicit allowlist.
					"OMS_CORS_ORIGINS": "https://trade.openexchange.dev",
				},
				WorkDir:     cfg.OmsProjectDir,
				DependsOn:   nodeNames(matchCluster),
				AutoRestart: true, RestartSec: 5, StopTimeout: 10,
				Artifact: cfg.OmsJar,
			},
			{
				Name: "market", Display: "Market Gateway", Role: RoleGateway, Port: 8081,
				ExtraPorts: []int{9091}, // Aeron UDP egress port
				Command: append(pinnedGatewayCmd("16-19"), "-cp", "match-gateway/target/match-gateway.jar",
					"com.match.infrastructure.gateway.MarketGatewayMain"),
				Env: map[string]string{
					"MATCH_PROJECT_DIR": cfg.ProjectDir,
					"EGRESS_PORT":       "9091",
					"GATEWAY_TYPE":      "market",
					// Market-data persistence (TimescaleDB): the DB is the source
					// of truth for candles/trades/ticker; the gateway falls back to
					// in-memory when it is absent. MARKET_PG_PASSWORD is inherited
					// from the admin service environment (systemd drop-in
					// admin.service.d/marketdata-db.conf) — without it the gateway
					// runs pure in-memory, exactly as before persistence existed.
					"MARKET_PG_URL":  "jdbc:postgresql://localhost:5432/marketdata",
					"MARKET_PG_USER": "market",
					// Edge fan-out (match edge/market-relay): the gateway
					// publishes each broadcast frame ONCE to the relay Worker,
					// which fans out to all public viewers at the Cloudflare
					// edge — origin upload stays flat in viewer count (the box
					// uplink is ~5 Mbit/s). The token FILE is the secret
					// (0600, outside the repo); with URL or token absent the
					// gateway serves viewers directly, exactly as before.
					"MARKET_EDGE_URL":        "wss://market-relay.emrebulutlar.workers.dev/publish",
					"MARKET_EDGE_TOKEN_FILE": marketEdgeTokenFile(),
				},
				WorkDir:     cfg.ProjectDir,
				DependsOn:   nodeNames(matchCluster),
				AutoRestart: true, RestartSec: 5, StopTimeout: 5,
				Artifact: cfg.GatewayJar,
			},
			{
				// Market simulator + demo canary (openexch/tools market-sim):
				// keeps the demo alive AND continuously proves the user path
				// end-to-end (orders, fills, market data, CORS). Health at
				// :8090/health. Pinned to the spare E-cores; pause it before
				// ad-hoc load tests (POST :8090/control {"pause":true}).
				Name: "sim", Display: "Market Simulator", Role: RoleInfra, Port: 8090,
				// -global-ops is profile-driven (0 = canary only, no maker load);
				// stop the sim (processes/sim/stop) to fully free cores 20-23.
				Command: append(pin("20-23"), cfg.SimBinary,
					"-mode=run", "-source=auto", fmt.Sprintf("-global-ops=%d", prof.SimGlobalOps)),
				Env: map[string]string{
					"OMS_URL":            "http://127.0.0.1:8080",
					"MARKET_WS_URL":      "ws://127.0.0.1:8081/ws",
					"SIM_HEALTH_ADDR":    "127.0.0.1:8090",
					"SIM_CORS_ORIGIN":    "https://trade.openexchange.dev",
					"SIM_PUBLIC_OMS_URL": "https://oms.openexchange.dev",
					// The edge-relay viewer path (market-relay Worker): the
					// canary watches the SAME public WS real viewers use, so
					// a frozen edge (half-open publisher, 2026-07-07) flips
					// admin_demo_healthy even while the local stack is fine.
					"SIM_EDGE_WS_URL": "wss://market.openexchange.dev/ws",
				},
				WorkDir:     filepath.Dir(cfg.SimBinary),
				DependsOn:   []string{"oms", "market"},
				AutoRestart: true, RestartSec: 10, StopTimeout: 15, // shutdown cancels sim quotes
				Artifact: cfg.SimBinary,
			},
		}...)

	// Assets Engine nodes (the money ledger — a SEPARATE Aeron cluster from the
	// matching engine), generated from the descriptor like the ME so its node
	// count is topology-driven. The launch spec stays FIXED (not profile-derived):
	// backoff idle + embedded driver + small heap + pinned to cores 20-23 (the
	// sim's E-cores) so it NEVER competes with the matching engine on 0-11.
	// Ports 9300+, state on tmpfs; the embedded driver self-cleans.
	aeAddresses := clusterAddresses(assetsCluster.NodeCount())
	services = append(services, assetsCluster.NodeServiceDefs(
		false, // embedded driver: no external driver services, ever
		func(i int) []string {
			return []string{
				"/usr/bin/taskset", "-c", "20-23",
				"/usr/bin/java",
				"-XX:+UseZGC", "-XX:+ZGenerational",
				"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
				"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
				"-Xmx512m", "-Xms512m",
				"-jar", cfg.AssetsJar,
			}
		},
		func(i int) []string { return nil }, // never invoked with external=false
		func(i int) map[string]string {
			return map[string]string{
				"CLUSTER_ADDRESSES":     aeAddresses,
				"CLUSTER_NODE":          strconv.Itoa(i),
				"CLUSTER_PORT_BASE":     strconv.Itoa(assetsCluster.PortBase),
				"BASE_DIR":              assetsCluster.NodeStateDir(i),
				"TRANSPORT_IDLE_MODE":   "backoff",  // never busy-spin on a shared box
				"TRANSPORT_DRIVER_MODE": "embedded", // self-contained, no external driver service
			}
		},
		func(i int) [][]string {
			return [][]string{{"mkdir", "-p", assetsCluster.NodeStateDir(i)}}
		},
	)...)

	// Settlement Bridge: ME settlement journal -> Assets Engine ingress
	// (assets/assets-bridge). A plain infra service like sim/driver — not part
	// of either Aeron cluster. Stateless: every epoch resumes from the AE's own
	// FeedPositionReport, so DependsOn only gates the FIRST start attempt
	// against a not-yet-up AE node; the bridge retries its own connections on
	// every subsequent failure without help from the process manager.
	const bridgeMetricsPort = 9600
	services = append(services, ServiceDef{
		Name: "bridge", Display: "Settlement Bridge", Role: RoleInfra, Port: bridgeMetricsPort,
		Command: []string{"/usr/bin/java", "-Xmx256m", "-jar", cfg.AssetsBridgeJar},
		Env: map[string]string{
			"BRIDGE_ME_JOURNAL_ARCHIVES":  cfg.BridgeJournalArchives,
			"BRIDGE_AE_CLUSTER_ADDRESSES": aeAddresses,
			"BRIDGE_AE_PORT_BASE":         strconv.Itoa(assetsCluster.PortBase),
			"BRIDGE_AE_EGRESS_ENDPOINT":   "127.0.0.1:9394",
			"BRIDGE_HALT_ON_GAP":          "true",
			"BRIDGE_METRICS_PORT":         strconv.Itoa(bridgeMetricsPort),
		},
		WorkDir:     cfg.AssetsProjectDir,
		DependsOn:   nodeNames(assetsCluster),
		AutoRestart: true, RestartSec: 10, StopTimeout: 10,
		Artifact: cfg.AssetsBridgeJar,
	})

	services = append(services, ServiceDef{
		Name: "admin", Display: "Admin Gateway", Role: RoleGateway, Port: 8082,
		// Admin is self — we don't manage ourselves, just report status
	})
	// Trading UI lives in separate repo (trading-ui)

	// Slice order IS the boot order; StartOrder mirrors it for display/debugging
	for i := range services {
		services[i].StartOrder = i + 1
	}

	if externalDriver {
		logger.Info("engine driver mode: external, media drivers managed as driver0-2", "profile", driverProfile)
	} else {
		logger.Info("engine driver mode: embedded")
	}

	return services
}

func (pm *ProcessManager) Shutdown() {
	// Signal all monitor goroutines to stop auto-restarting
	close(pm.stopChan)

	// Also close every per-process stopChan to prevent any in-flight auto-restarts
	for _, def := range pm.servicesSnapshot() {
		if def.Name == "admin" {
			continue
		}
		proc := pm.proc(def.Name)
		if proc == nil {
			continue
		}
		proc.mu.Lock()
		if proc.stopChan != nil {
			select {
			case <-proc.stopChan:
			default:
				close(proc.stopChan)
			}
		}
		proc.mu.Unlock()
	}

	// Brief pause to let any in-flight restarts see the closed channels
	time.Sleep(500 * time.Millisecond)
}

// --- Public API ---

// List returns live info for all managed services
func (pm *ProcessManager) List() []ProcessInfo {
	defs := pm.servicesSnapshot()
	result := make([]ProcessInfo, len(defs))
	for i, def := range defs {
		result[i] = pm.getInfo(def)
	}
	return result
}

// Get returns live info for a single service
func (pm *ProcessManager) Get(name string) *ProcessInfo {
	def := pm.findDef(name)
	if def == nil {
		return nil
	}
	info := pm.getInfo(*def)
	return &info
}

// Start a service (with dependency check)
func (pm *ProcessManager) Start(name string) error {
	def := pm.findDef(name)
	if def == nil {
		return fmt.Errorf("unknown service: %s", name)
	}

	if name == "admin" {
		return fmt.Errorf("admin gateway manages itself via rebuild-admin endpoint")
	}

	if len(def.Command) == 0 {
		return fmt.Errorf("service %s has no command configured", name)
	}

	// Operator intent: this service should be running. Recorded even if the
	// start below fails — a service that should run stays desired-running so a
	// reboot retries it. (Crash/disarm never touches this; see setDesired.)
	pm.setDesired(name, config.DesiredRunning)

	// Check if already running
	proc := pm.proc(name)
	if proc == nil {
		return fmt.Errorf("unknown service: %s", name)
	}
	proc.mu.Lock()
	if proc.running {
		proc.mu.Unlock()
		return fmt.Errorf("%s is already running (PID %d)", name, proc.pid)
	}
	proc.mu.Unlock()

	// Check dependencies
	for _, dep := range def.DependsOn {
		isRunning := false
		if depProc := pm.proc(dep); depProc != nil {
			depProc.mu.Lock()
			isRunning = depProc.running
			depProc.mu.Unlock()
		}
		if !isRunning {
			return fmt.Errorf("dependency %q is not running (required by %s)", dep, name)
		}
	}

	pm.rearm(name) // explicit start re-arms the crash-loop cap
	return pm.startProcess(*def)
}

// Stop a service (with reverse dependency check)
func (pm *ProcessManager) Stop(name string) error {
	def := pm.findDef(name)
	if def == nil {
		return fmt.Errorf("unknown service: %s", name)
	}

	if name == "admin" {
		return fmt.Errorf("admin gateway manages itself via rebuild-admin endpoint")
	}

	// Operator intent: this service should stay stopped across reboots.
	pm.setDesired(name, config.DesiredStopped)

	// Check if anything depends on us and is still running
	dependents := pm.findDependents(name)
	runningDeps := []string{}
	for _, d := range dependents {
		dProc := pm.proc(d)
		if dProc == nil {
			continue
		}
		dProc.mu.Lock()
		if dProc.running {
			runningDeps = append(runningDeps, d)
		}
		dProc.mu.Unlock()
	}
	if len(runningDeps) > 0 {
		return fmt.Errorf("cannot stop %s: services still depend on it: %s (stop them first or use force-stop)",
			name, strings.Join(runningDeps, ", "))
	}

	return pm.stopProcess(name, false)
}

// ForceStop bypasses dependency checks
func (pm *ProcessManager) ForceStop(name string) error {
	if pm.findDef(name) == nil {
		return fmt.Errorf("unknown service: %s", name)
	}
	if name == "admin" {
		return fmt.Errorf("admin gateway manages itself via rebuild-admin endpoint")
	}
	return pm.stopProcess(name, true)
}

// Restart a service
func (pm *ProcessManager) Restart(name string) error {
	def := pm.findDef(name)
	if def == nil {
		return fmt.Errorf("unknown service: %s", name)
	}
	if name == "admin" {
		return fmt.Errorf("admin gateway manages itself via rebuild-admin endpoint")
	}

	proc := pm.proc(name)
	if proc == nil {
		return fmt.Errorf("unknown service: %s", name)
	}
	proc.mu.Lock()
	wasRunning := proc.running
	proc.mu.Unlock()

	if wasRunning {
		if err := pm.stopProcess(name, true); err != nil {
			return fmt.Errorf("failed to stop %s for restart: %w", name, err)
		}
		// Wait for port release if the service binds a port
		if def.Port > 0 {
			if err := pm.waitForPortFree(def.Port, 20*time.Second); err != nil {
				pm.log.Warn("port not free after stop, proceeding anyway", "port", def.Port, "err", err)
			}
		} else {
			// Brief pause for non-port services (Aeron cleanup)
			time.Sleep(2 * time.Second)
		}
	}

	pm.rearm(name) // explicit restart re-arms the crash-loop cap
	return pm.startProcess(*def)
}

// StartAll starts services in dependency order
func (pm *ProcessManager) StartAll() []ActionResult {
	pm.setDesiredAll(config.DesiredRunning) // explicit bulk intent: the stack should be up
	results := []ActionResult{}
	ordered := pm.bootOrder()

	for _, def := range ordered {
		if def.Name == "admin" || len(def.Command) == 0 {
			continue
		}

		proc := pm.proc(def.Name)
		if proc == nil {
			continue
		}
		proc.mu.Lock()
		isRunning := proc.running
		proc.mu.Unlock()

		if isRunning {
			results = append(results, ActionResult{Service: def.Name, Action: "start", Success: true, Error: "already running"})
			continue
		}

		pm.rearm(def.Name) // start-all is explicit intent
		err := pm.startProcess(def)
		result := ActionResult{Service: def.Name, Action: "start", Success: err == nil}
		if err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
		time.Sleep(startStagger)
	}

	return results
}

// StopAll stops services in reverse dependency order
func (pm *ProcessManager) StopAll() []ActionResult {
	pm.setDesiredAll(config.DesiredStopped) // explicit bulk intent: keep the stack down
	results := []ActionResult{}
	ordered := pm.shutdownOrder()

	for _, def := range ordered {
		if def.Name == "admin" || len(def.Command) == 0 {
			continue
		}

		proc := pm.proc(def.Name)
		if proc == nil {
			continue
		}
		proc.mu.Lock()
		isRunning := proc.running
		proc.mu.Unlock()

		if !isRunning {
			results = append(results, ActionResult{Service: def.Name, Action: "stop", Success: true, Error: "already stopped"})
			continue
		}

		err := pm.stopProcess(def.Name, true) // force during bulk stop
		result := ActionResult{Service: def.Name, Action: "stop", Success: err == nil}
		if err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
	}

	return results
}

// ActionResult is the bulk-operation outcome type (defined in agent).
type ActionResult = agent.ActionResult

// --- Process Lifecycle ---

// StartUnchecked starts a service without the dependency check — for
// orchestration callers (operations) that sequence dependencies themselves.
func (pm *ProcessManager) StartUnchecked(name string) error {
	def := pm.findDef(name)
	if def == nil {
		return fmt.Errorf("unknown service: %s", name)
	}
	if len(def.Command) == 0 {
		return fmt.Errorf("service %s has no command configured", name)
	}
	pm.rearm(name) // internal callers (operations) are explicit intent too
	return pm.startProcess(*def)
}

func (pm *ProcessManager) startProcess(def ServiceDef) error {
	return pm.startProcessInner(def, true)
}

func (pm *ProcessManager) startProcessNoRotate(def ServiceDef) error {
	return pm.startProcessInner(def, false)
}

func (pm *ProcessManager) startProcessInner(def ServiceDef, rotateLogs bool) error {
	proc := pm.proc(def.Name)
	if proc == nil {
		return fmt.Errorf("unknown service %s (removed by a catalog reload?)", def.Name)
	}

	// Phase 1: Check state and claim the start (hold lock briefly)
	proc.mu.Lock()
	if proc.running {
		proc.mu.Unlock()
		return fmt.Errorf("%s is already running", def.Name)
	}
	if proc.starting {
		proc.mu.Unlock()
		return fmt.Errorf("%s is already starting", def.Name)
	}
	proc.starting = true
	proc.status = "starting"
	proc.mu.Unlock()

	// Phase 2: Pre-start work WITHOUT holding the lock (port wait, cleanup, etc.)
	// This allows status queries and stop commands to proceed during preparation.
	var startErr error

	// Pre-start artifact check (#45): relaunching into a missing jar exits
	// instantly ("Unable to access jarfile"), burns the crash-loop cap in
	// seconds and disarms auto-restart — that turned a rebuild-gateway mvn
	// clean into a 10-minute full outage on 2026-07-06. One refused start
	// with the cause instead. This is the single start funnel, so the check
	// covers API starts, StartAll, auto-restart and cascade recovery alike.
	if def.Artifact != "" {
		if _, err := os.Stat(def.Artifact); err != nil {
			refuse := fmt.Errorf("artifact missing: %s — rebuild in progress or failed? start refused; restore the artifact, then start explicitly", def.Artifact)
			proc.mu.Lock()
			proc.status = "failed"
			proc.lastError = refuse.Error()
			proc.starting = false
			proc.mu.Unlock()
			pm.log.Warn("refused start", "service", def.Name, "err", refuse)
			return refuse
		}
	}

	// Orphan-driver refusal (#42): reaching here means this driver is tracked
	// stopped, so a live pid in <DriverDir>.pid is an untracked orphan. Launching
	// over it would exit 0 (launch-driver.sh is idempotent), read as an instant
	// crash, and burn the crash-loop cap into disarm while the real driver keeps
	// running — the exact illusion behind the 2026-07-06 node0 outage. One clear
	// refusal instead; force-stop kills the orphan and clears the state.
	if def.DriverDir != "" {
		if opid, alive := driverPidFileAlive(def.DriverDir); alive {
			err := fmt.Errorf("orphan media driver alive (pid %d) holding %s — force-stop %s (kills the orphan too), then start",
				opid, def.DriverDir, def.Name)
			proc.mu.Lock()
			proc.status = "failed"
			proc.lastError = err.Error()
			proc.starting = false
			proc.mu.Unlock()
			pm.log.Warn("refused start", "service", def.Name, "err", err)
			return err
		}
	}

	// Driver gate FIRST, before any cleanup touches node state on disk: a gated
	// service must not start (or mutate its archive dirs) unless its driver is
	// running, stable, and has published cnc.dat (#17).
	if def.GatedBy != "" {
		if err := pm.waitForGate(def); err != nil {
			proc.mu.Lock()
			proc.status = "failed"
			proc.lastError = err.Error()
			proc.starting = false
			proc.mu.Unlock()
			pm.log.Warn("refused start", "service", def.Name, "err", err)
			return err
		}
	}

	// Clean stale Aeron state for cluster nodes
	if def.Role == RoleClusterNode {
		pm.cleanStaleAeronState(def.Name)
	}

	// Kill orphaned processes on ALL ports (main + extra) BEFORE waiting for ports.
	// An orphan may hold both the HTTP port and Aeron egress port — must kill first.
	if def.Port > 0 {
		pm.killOrphanedPortHolder(def.Port, def.Name)
	}
	for _, extraPort := range def.ExtraPorts {
		pm.killOrphanedPortHolder(extraPort, def.Name)
	}

	// Clean stale Aeron MediaDriver dirs for gateways
	if def.Role == RoleGateway {
		pm.cleanStaleGatewayAeron(def.Name)
	}

	// Wait for port to be free (gateways bind HTTP ports)
	if def.Port > 0 {
		if err := pm.waitForPortFree(def.Port, 15*time.Second); err != nil {
			startErr = fmt.Errorf("port %d not free for %s: %w", def.Port, def.Name, err)
		}
	}
	// Also wait for extra ports (Aeron egress)
	if startErr == nil {
		for _, extraPort := range def.ExtraPorts {
			if err := pm.waitForPortFree(extraPort, 10*time.Second); err != nil {
				startErr = fmt.Errorf("extra port %d not free for %s: %w", extraPort, def.Name, err)
				break
			}
		}
	}

	// Run pre-start commands
	if startErr == nil {
		for _, preCmd := range def.PreStart {
			if len(preCmd) == 0 {
				continue
			}
			cmd := exec.Command(preCmd[0], preCmd[1:]...)
			cmd.Dir = def.WorkDir
			if out, err := cmd.CombinedOutput(); err != nil {
				startErr = fmt.Errorf("pre-start command %v failed: %s: %w", preCmd, string(out), err)
				break
			}
		}
	}

	// Wait for a required file (e.g. the external media driver's cnc.dat). Warn and
	// continue on timeout — the Java side re-waits with its own actionable error.
	// Gated services skip this: waitForGate already required the file strictly.
	if startErr == nil && def.WaitForFile != "" && def.GatedBy == "" {
		if err := waitForFile(def.WaitForFile, 15*time.Second); err != nil {
			pm.log.Warn("wait-for-file failed, starting anyway", "file", def.WaitForFile, "service", def.Name, "err", err)
		}
	}

	// If pre-start failed, mark as failed and return
	if startErr != nil {
		proc.mu.Lock()
		proc.status = "failed"
		proc.lastError = startErr.Error()
		proc.starting = false
		proc.mu.Unlock()
		return startErr
	}

	// Phase 3: Actually start the process (hold lock for state mutation)
	proc.mu.Lock()
	defer func() {
		proc.starting = false
		proc.mu.Unlock()
	}()

	// Re-check: someone might have started it while we were doing pre-start work
	if proc.running {
		return fmt.Errorf("%s is already running (started while preparing)", def.Name)
	}

	// Rotate log file (skip on auto-restart to preserve crash context)
	logPath := filepath.Join(pm.logDir, def.Name+".log")
	if rotateLogs {
		pm.rotateLog(logPath)
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		proc.status = "failed"
		proc.lastError = fmt.Sprintf("failed to open log file %s: %v", logPath, err)
		return fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	// Build command
	cmd := exec.Command(def.Command[0], def.Command[1:]...)
	cmd.Dir = def.WorkDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Set process group so we can kill the whole tree
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Environment: inherit current env + add overrides
	env := os.Environ()
	for k, v := range def.Env {
		env = append(env, k+"="+v)
	}
	cmd.Env = env

	// Start the process
	if err := cmd.Start(); err != nil {
		logFile.Close()
		proc.status = "failed"
		proc.lastError = fmt.Sprintf("failed to start: %v", err)
		return fmt.Errorf("failed to start %s: %w", def.Name, err)
	}

	proc.cmd = cmd
	proc.pid = cmd.Process.Pid
	proc.running = true
	proc.startedAt = time.Now()
	proc.status = "running"
	proc.lastError = ""
	proc.logFile = logFile
	proc.stopChan = make(chan struct{})

	// Write PID file
	pm.writePID(def.Name, proc.pid)

	pm.log.Info("started service", "service", def.Name, "pid", proc.pid)
	pm.emitEvent(agent.EventStarted, def.Name, proc.pid, "")

	// Monitor process in background (handles crash + auto-restart)
	go pm.monitor(def, proc)

	return nil
}

func (pm *ProcessManager) stopProcess(name string, force bool) error {
	proc := pm.proc(name)
	if proc == nil {
		return fmt.Errorf("unknown service %s (removed by a catalog reload?)", name)
	}

	// Force-stopping a driver also kills an orphan aeronmd named by the launch
	// script's pid file (#42): the orphan state (live driver, tracked stopped)
	// is otherwise unreachable by any API verb, leaving runbook 1's recovery
	// sequence stuck behind manual kills.
	if force {
		if def := pm.findDef(name); def != nil && def.DriverDir != "" {
			proc.mu.Lock()
			trackedPid := proc.pid
			proc.mu.Unlock()
			if opid, alive := driverPidFileAlive(def.DriverDir); alive && opid != trackedPid {
				pm.log.Warn("force-stop killing orphan driver from pid file",
					"service", name, "pid", opid, "dir", def.DriverDir)
				syscall.Kill(-opid, syscall.SIGTERM)
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) && isProcessAlive(opid) {
					time.Sleep(100 * time.Millisecond)
				}
				if isProcessAlive(opid) {
					syscall.Kill(-opid, syscall.SIGKILL)
				}
			}
		}
	}

	proc.mu.Lock()

	// Always cancel pending auto-restarts, even if process appears stopped
	if proc.stopChan != nil {
		select {
		case <-proc.stopChan:
		default:
			close(proc.stopChan)
		}
	}

	if !proc.running || proc.cmd == nil {
		// Maybe an adopted process — try killing by PID
		if proc.pid > 0 {
			proc.status = "stopping"
			pid := proc.pid
			proc.mu.Unlock()
			syscall.Kill(-pid, syscall.SIGTERM) // Kill process group
			time.Sleep(2 * time.Second)
			// Check if dead
			if isProcessAlive(pid) {
				syscall.Kill(-pid, syscall.SIGKILL)
				time.Sleep(500 * time.Millisecond)
			}
			proc.mu.Lock()
			proc.running = false
			proc.pid = 0
			proc.status = "stopped"
			proc.mu.Unlock()
			pm.removePID(name)
			pm.log.Info("stopped adopted process", "service", name)
			pm.emitEvent(agent.EventStopped, name, pid, "adopted process")
			return nil
		}
		proc.status = "stopped"
		proc.mu.Unlock()
		return nil // Not an error — just confirms it's stopped
	}

	proc.status = "stopping"
	pid := proc.pid
	proc.mu.Unlock()

	// Graceful shutdown: SIGTERM to process group
	def := pm.findDef(name)
	timeout := 5
	if def != nil && def.StopTimeout > 0 {
		timeout = def.StopTimeout
	}

	syscall.Kill(-pid, syscall.SIGTERM)

	// Wait for graceful exit
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		if !isProcessAlive(pid) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Force kill if still alive
	if isProcessAlive(pid) {
		pm.log.Warn("force killing service", "service", name, "pid", pid)
		syscall.Kill(-pid, syscall.SIGKILL)
		// Wait for the process to actually die (up to 5s after SIGKILL)
		killDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(killDeadline) {
			if !isProcessAlive(pid) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if isProcessAlive(pid) {
			pm.log.Warn("still alive after sigkill and 5s wait", "service", name, "pid", pid)
		}
	}

	proc.mu.Lock()
	proc.running = false
	proc.pid = 0
	proc.status = "stopped"
	if proc.logFile != nil {
		proc.logFile.Close()
		proc.logFile = nil
	}
	proc.mu.Unlock()

	pm.removePID(name)
	pm.log.Info("stopped service", "service", name)
	pm.emitEvent(agent.EventStopped, name, pid, "")

	return nil
}

func (pm *ProcessManager) monitor(def ServiceDef, proc *managedProcess) {
	if proc.cmd == nil {
		return
	}

	// Wait for process to exit
	err := proc.cmd.Wait()

	proc.mu.Lock()
	wasRunning := proc.running
	stopChan := proc.stopChan
	proc.running = false
	oldPid := proc.pid
	proc.pid = 0

	if proc.logFile != nil {
		proc.logFile.Close()
		proc.logFile = nil
	}
	proc.mu.Unlock()

	pm.removePID(def.Name)

	// Check if this was an intentional stop
	select {
	case <-stopChan:
		// Intentional stop — don't restart
		proc.mu.Lock()
		proc.status = "stopped"
		proc.mu.Unlock()
		pm.log.Info("service stopped intentionally", "service", def.Name, "pid", oldPid)
		return
	default:
	}

	// Also check if the whole PM is shutting down
	select {
	case <-pm.stopChan:
		proc.mu.Lock()
		proc.status = "stopped"
		proc.mu.Unlock()
		return
	default:
	}

	exitMsg := "unknown"
	if err != nil {
		exitMsg = err.Error()
	}
	if !wasRunning {
		exitMsg += " (was not marked running)"
	}
	pm.handleCrash(def, proc, oldPid, exitMsg, stopChan)
}

// handleCrash runs the shared post-crash protocol: lastError capture, rapid
// crash-loop cap (#17), driver→node cascades, and the auto-restart. Called by
// monitor() for processes we spawned and by the adopted-process watchdog (#13)
// for processes re-attached after an admin restart (stopChan nil there — a nil
// channel never fires in a select, and the pre-restart status re-check covers
// cancellation for adopted processes).
func (pm *ProcessManager) handleCrash(
	def ServiceDef, proc *managedProcess, oldPid int, exitMsg string, stopChan chan struct{}) {

	crashCause := fmt.Sprintf("crashed (exit: %s)", exitMsg)
	if tail := tailLogSnippet(filepath.Join(pm.logDir, def.Name+".log")); tail != "" {
		crashCause += " — log: " + tail
	}
	pm.log.Error("service crashed", "service", def.Name, "pid", oldPid, "cause", crashCause)

	// Record the crash for the rapid-loop cap (#17): count crashes inside the
	// sliding window, not lifetime restarts.
	now := time.Now()
	proc.mu.Lock()
	kept := proc.crashTimes[:0]
	for _, t := range proc.crashTimes {
		if now.Sub(t) < rapidCrashWindow {
			kept = append(kept, t)
		}
	}
	proc.crashTimes = append(kept, now)
	rapidCrashes := len(proc.crashTimes)
	proc.lastError = crashCause
	proc.mu.Unlock()

	pm.emitEvent(agent.EventCrashed, def.Name, oldPid, crashCause)

	// Auto-restart if enabled (with crash-loop cap)
	if def.AutoRestart {
		// Crash cascade (media driver → node): the node's shared-memory IPC died with
		// the driver, so stop it BEFORE restarting the driver. It is started again
		// below once the driver is back, giving deterministic driver-then-node order.
		for _, target := range def.RestartCascades {
			pm.log.Warn("force-stopping dependent after crash", "service", def.Name, "dependent", target)
			pm.emitEvent(agent.EventCascadeStop, target, 0, "cascade from "+def.Name)
			if err := pm.stopProcess(target, true); err != nil {
				pm.log.Error("failed to stop dependent during crash cascade", "dependent", target, "service", def.Name, "err", err)
			}
		}

		// Rapid-loop cap: after rapidCrashMax crashes inside rapidCrashWindow,
		// STOP retrying and require an explicit start to re-arm. An unattended
		// restart loop against a broken environment is what let nodes write a
		// little archive and die mid-write for hours on 2026-07-03 (#17).
		if rapidCrashes >= rapidCrashMax {
			msg := fmt.Sprintf("crash-looped %d times within %s; auto-restart disarmed — fix the cause, then start explicitly. Last crash: %s",
				rapidCrashes, rapidCrashWindow, crashCause)
			proc.mu.Lock()
			proc.status = "failed"
			proc.lastError = msg
			proc.mu.Unlock()
			pm.log.Error("service failed, auto-restart disarmed", "service", def.Name, "reason", msg)
			pm.emitEvent(agent.EventDisarmed, def.Name, 0, msg)
			return
		}

		restartSec := def.RestartSec
		if restartSec <= 0 {
			restartSec = 5
		}

		proc.mu.Lock()
		proc.status = "restarting"
		proc.mu.Unlock()

		pm.log.Warn("will restart service", "service", def.Name, "delay_sec", restartSec)

		select {
		case <-time.After(time.Duration(restartSec) * time.Second):
		case <-pm.stopChan:
			return
		case <-stopChan:
			return
		}

		// Check if someone else started it meanwhile, or an explicit stop
		// cancelled the restart (status left "restarting" — the only stopChan
		// equivalent adopted processes have).
		proc.mu.Lock()
		if proc.starting || proc.running || proc.status != "restarting" {
			state := proc.status
			if proc.running {
				state = "running"
			} else if proc.starting {
				state = "starting"
			}
			proc.mu.Unlock()
			pm.log.Info("skipping auto-restart", "service", def.Name, "state", state)
			return
		}
		proc.restartCount++
		proc.mu.Unlock()

		pm.log.Warn("auto-restarting service", "service", def.Name, "attempt", proc.restartCount)
		// Use startProcessNoRotate to preserve crash logs
		if err := pm.startProcessNoRotate(def); err != nil {
			pm.log.Error("failed to restart service", "service", def.Name, "err", err)
			proc.mu.Lock()
			proc.status = "failed"
			proc.lastError = fmt.Sprintf("auto-restart failed: %v", err)
			proc.mu.Unlock()
		} else {
			// Bring cascade-stopped dependents back now that we are up again
			// (their start waits for our WaitForFile/cnc.dat readiness signal)
			for _, target := range def.RestartCascades {
				tdef := pm.findDef(target)
				if tdef == nil {
					continue
				}
				pm.log.Info("restarting dependent after recovery", "dependent", target, "service", def.Name)
				if err := pm.startProcessNoRotate(*tdef); err != nil {
					pm.log.Error("failed to restart dependent after recovery", "dependent", target, "service", def.Name, "err", err)
				}
			}
		}
	} else {
		proc.mu.Lock()
		proc.status = "crashed"
		proc.mu.Unlock()
	}
}

// --- Process Adoption (re-discover after admin restart) ---

func (pm *ProcessManager) adoptExisting() {
	for _, def := range pm.servicesSnapshot() {
		// Special case: admin is always "us"
		if def.Name == "admin" {
			proc := pm.proc(def.Name)
			if proc == nil {
				continue
			}
			proc.mu.Lock()
			proc.pid = os.Getpid()
			proc.running = true
			proc.startedAt = time.Now() // approximate
			proc.status = "running"
			proc.mu.Unlock()
			continue
		}

		pid := pm.readPID(def.Name)
		if pid <= 0 {
			continue
		}

		if isProcessAlive(pid) {
			proc := pm.proc(def.Name)
			if proc == nil {
				continue
			}
			proc.mu.Lock()
			proc.pid = pid
			proc.running = true
			proc.status = "running"
			proc.startedAt = getProcessStartTime(pid)
			proc.mu.Unlock()
			pm.log.Info("adopted service", "service", def.Name, "pid", pid)
			pm.emitEvent(agent.EventAdopted, def.Name, pid, "")
		} else {
			// Stale PID file
			pm.removePID(def.Name)
		}
	}

	// After adoption, kill orphaned processes holding ports that belong to adopted services.
	// This handles the race where auto-restart spawns a duplicate during admin restart.
	pm.cleanupOrphansAfterAdoption()
}

// cleanupOrphansAfterAdoption kills processes holding service ports that aren't the adopted PID.
// Runs once at startup to catch orphans from the previous admin instance's auto-restart race.
func (pm *ProcessManager) cleanupOrphansAfterAdoption() {
	for _, def := range pm.servicesSnapshot() {
		if def.Name == "admin" {
			continue
		}
		proc := pm.proc(def.Name)
		if proc == nil {
			continue
		}
		proc.mu.Lock()
		running := proc.running
		proc.mu.Unlock()
		if !running {
			continue
		}

		// Check main port
		if def.Port > 0 {
			pm.killOrphanedPortHolder(def.Port, def.Name)
		}
		// Check extra ports (Aeron egress)
		for _, extraPort := range def.ExtraPorts {
			pm.killOrphanedPortHolder(extraPort, def.Name)
		}
	}
}

// --- PID File Management ---

func (pm *ProcessManager) writePID(name string, pid int) {
	path := filepath.Join(pm.pidDir, name+".pid")
	os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644)
}

func (pm *ProcessManager) readPID(name string) int {
	path := filepath.Join(pm.pidDir, name+".pid")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

func (pm *ProcessManager) removePID(name string) {
	path := filepath.Join(pm.pidDir, name+".pid")
	os.Remove(path)
}

// --- Aeron Cleanup ---

// cleanStaleAeronState removes stale mark files and media driver directories
// that prevent a node from restarting after a crash
func (pm *ProcessManager) cleanStaleAeronState(name string) {
	// Matching-engine nodes are named node<i> for any topology; the assets
	// engine's embedded driver self-cleans (dirDeleteOnStart), so ae<i> nodes
	// deliberately do not match here.
	var nodeId int
	matched, err := fmt.Sscanf(name, "node%d", &nodeId)
	if matched == 1 && err == nil {
		// Clean stale MediaDriver directory — but NEVER while an external driver
		// process owns it (external mode shares this dir between driverN and nodeN;
		// deleting it under a live driver corrupts the IPC files). Tracked state
		// alone is NOT trusted: it reads stopped during driver crash-loops and
		// adoption gaps, which is how the 2026-07-06 rolling update deleted
		// node0's live dir (#42) — the pid-file ground truth must agree.
		driverRunning := false
		if dproc := pm.proc(fmt.Sprintf("driver%d", nodeId)); dproc != nil {
			dproc.mu.Lock()
			driverRunning = dproc.running
			dproc.mu.Unlock()
		}
		// Embedded-mode fallback (ag#68): with no external driver there is no pid
		// file, so canDeleteDriverDir also refuses while the owning node runs.
		nodeRunning := false
		if nproc := pm.proc(name); nproc != nil {
			nproc.mu.Lock()
			nodeRunning = nproc.running
			nproc.mu.Unlock()
		}
		driverDir := driverDirPath(nodeId)
		if ok, reason := canDeleteDriverDir(driverDir, driverRunning, nodeRunning); ok {
			if _, err := os.Stat(driverDir); err == nil {
				os.RemoveAll(driverDir)
				pm.log.Info("cleaned stale media driver dir", "dir", driverDir)
			}
		} else if !driverRunning {
			// Tracked stopped but a live driver holds the dir: the #42 state.
			pm.log.Error("refusing to delete media driver dir (#42 guard)",
				"dir", driverDir, "reason", reason)
		}

		// Clean stale mark files and locks
		nodeDir := filepath.Join(pm.matchStateDir, name)
		patterns := []string{
			nodeDir + "/cluster/cluster-mark*.dat",
			nodeDir + "/cluster/*.lck",
			nodeDir + "/archive/archive-mark.dat",
		}
		for _, pattern := range patterns {
			matches, _ := filepath.Glob(pattern)
			for _, m := range matches {
				os.Remove(m)
				pm.log.Info("cleaned stale file", "file", m)
			}
		}
	}

	// Backup node — its state lives on DISK under ProjectDir/backup (match#36/#9),
	// NOT the old /dev/shm/aeron-cluster/backup (that dir was never used by the app)
	if name == "backup" && pm.projectDir != "" {
		backupDir := filepath.Join(pm.projectDir, "backup")
		patterns := []string{
			backupDir + "/cluster/cluster-mark*.dat",
			backupDir + "/cluster/*.lck",
			backupDir + "/archive/archive-mark.dat",
		}
		for _, pattern := range patterns {
			matches, _ := filepath.Glob(pattern)
			for _, m := range matches {
				os.Remove(m)
			}
		}
	}
}

// waitForGate blocks until def.GatedBy (the node's media driver) is running,
// has been up for gateStableFor, and def.WaitForFile (cnc.dat) exists. It
// fails fast when the driver is stopped/failed and is not coming back; a
// driver that is starting/restarting gets until gateTimeout to stabilize. A
// crash-looping driver never stays up gateStableFor, so the gate refuses the
// node instead of letting it write archive state against a flapping driver.
func (pm *ProcessManager) waitForGate(def ServiceDef) error {
	gproc := pm.proc(def.GatedBy)
	if gproc == nil {
		return fmt.Errorf("refusing to start %s: gating service %q is unknown", def.Name, def.GatedBy)
	}
	deadline := time.Now().Add(gateTimeout)
	for {
		gproc.mu.Lock()
		running := gproc.running
		startedAt := gproc.startedAt
		status := gproc.status
		lastErr := gproc.lastError
		gproc.mu.Unlock()

		if !running && status != "starting" && status != "restarting" {
			reason := fmt.Sprintf("refusing to start %s: %s is not running (status=%s)", def.Name, def.GatedBy, status)
			if lastErr != "" {
				reason += " — " + lastErr
			}
			return fmt.Errorf("%s", reason)
		}

		if running && !startedAt.IsZero() && time.Since(startedAt) >= gateStableFor {
			if def.WaitForFile == "" {
				return nil
			}
			if _, err := os.Stat(def.WaitForFile); err == nil {
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("refusing to start %s: %s did not become stable within %s (must be up %s with %s present) — driver flapping or misconfigured? check its log and lastError",
				def.Name, def.GatedBy, gateTimeout, gateStableFor, def.WaitForFile)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// tailLogSnippet returns the last few non-empty lines of a log file as a
// single truncated line, for surfacing crash causes in lastError.
func tailLogSnippet(logPath string) string {
	f, err := os.Open(logPath)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return ""
	}
	const window = 4096
	off := st.Size() - window
	if off < 0 {
		off = 0
	}
	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil && err != io.EOF {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(buf)), "\n")
	start := len(lines) - 3
	if start < 0 {
		start = 0
	}
	var parts []string
	for _, l := range lines[start:] {
		if l = strings.TrimSpace(l); l != "" {
			parts = append(parts, l)
		}
	}
	snippet := strings.Join(parts, " | ")
	if len(snippet) > 300 {
		snippet = "…" + snippet[len(snippet)-300:]
	}
	return snippet
}

// rearm clears the crash-loop window and lastError before an EXPLICIT start,
// so a service disarmed by the rapid-restart cap can be brought back once the
// underlying cause is fixed. Auto-restarts never re-arm (the cap would be moot).
func (pm *ProcessManager) rearm(name string) {
	proc := pm.proc(name)
	if proc == nil {
		return
	}
	proc.mu.Lock()
	proc.crashTimes = nil
	proc.lastError = ""
	proc.mu.Unlock()
}

// waitForFile polls until the file exists or the timeout elapses.
func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("not present after %s", timeout)
}

// isPortInUse checks both TCP and UDP for a port being in use.
func isPortInUse(port int) bool {
	for _, proto := range []string{"-t", "-u"} {
		cmd := exec.Command("ss", proto+"lnH", fmt.Sprintf("sport = :%d", port))
		out, err := cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			return true
		}
	}
	return false
}

// waitForPortFree polls until a port (TCP or UDP) is no longer in use.
// Logs what PID is holding the port for easier debugging.
func (pm *ProcessManager) waitForPortFree(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	logged := false
	for time.Now().Before(deadline) {
		if !isPortInUse(port) {
			return nil
		}
		if !logged {
			holder := pm.findPortHolder(port)
			if holder != "" {
				pm.log.Info("port held, waiting for release", "port", port, "holder", holder, "timeout", timeout)
			} else {
				pm.log.Info("port still in use, waiting for release", "port", port, "timeout", timeout)
			}
			logged = true
		}
		time.Sleep(500 * time.Millisecond)
	}
	holder := pm.findPortHolder(port)
	return fmt.Errorf("port %d still in use after %s (holder: %s)", port, timeout, holder)
}

// findPortHolder returns a description of the process holding a port (TCP or UDP).
func (pm *ProcessManager) findPortHolder(port int) string {
	for _, proto := range []string{"-t", "-u"} {
		cmd := exec.Command("ss", proto+"lnpH", fmt.Sprintf("sport = :%d", port))
		out, err := cmd.Output()
		if err != nil {
			continue
		}
		line := strings.TrimSpace(string(out))
		if line == "" {
			continue
		}
		// ss -p output includes something like: users:(("java",pid=12345,fd=6))
		if idx := strings.Index(line, "users:"); idx >= 0 {
			return strings.TrimSpace(line[idx:])
		}
		return line
	}
	return "none"
}

// killOrphanedPortHolder finds and kills any process holding a port that isn't tracked by the PM.
// This handles orphaned gateway processes left behind from failed restarts.
func (pm *ProcessManager) killOrphanedPortHolder(port int, serviceName string) {
	cmd := exec.Command("ss", "-tlnpH", fmt.Sprintf("sport = :%d", port))
	out, _ := cmd.Output()
	if len(strings.TrimSpace(string(out))) == 0 {
		// Also check UDP (Aeron egress uses UDP)
		cmd = exec.Command("ss", "-ulnpH", fmt.Sprintf("sport = :%d", port))
		out, _ = cmd.Output()
	}
	if len(strings.TrimSpace(string(out))) == 0 {
		return
	}

	// Extract PID from ss output like: users:(("java",pid=12345,fd=6))
	line := string(out)
	pidIdx := strings.Index(line, "pid=")
	if pidIdx < 0 {
		return
	}
	pidStr := line[pidIdx+4:]
	if commaIdx := strings.IndexAny(pidStr, ",)"); commaIdx > 0 {
		pidStr = pidStr[:commaIdx]
	}
	pid, err := strconv.Atoi(strings.TrimSpace(pidStr))
	if err != nil || pid <= 0 {
		return
	}

	// Check if this PID is the one we're tracking
	proc := pm.proc(serviceName)
	if proc == nil {
		return
	}
	proc.mu.Lock()
	trackedPID := proc.pid
	proc.mu.Unlock()

	if pid == trackedPID {
		return // It's our tracked process, not an orphan
	}

	// It's an orphan — kill it
	pm.log.Warn("killing orphaned process holding port", "pid", pid, "port", port, "service", serviceName)
	syscall.Kill(-pid, syscall.SIGTERM)
	time.Sleep(2 * time.Second)
	if isProcessAlive(pid) {
		syscall.Kill(-pid, syscall.SIGKILL)
		time.Sleep(500 * time.Millisecond)
	}

	// Clean its Aeron directory
	prefix := fmt.Sprintf("aeron-%s-%d", serviceName, pid)
	dirPath := filepath.Join("/dev/shm", prefix)
	if _, err := os.Stat(dirPath); err == nil {
		os.RemoveAll(dirPath)
		pm.log.Info("cleaned orphan aeron dir", "dir", dirPath)
	}
}

// cleanStaleGatewayAeron removes stale MediaDriver directories for gateway processes
func (pm *ProcessManager) cleanStaleGatewayAeron(name string) {
	// Gateway MediaDriver dirs are named aeron-{name}-{pid}
	// Clean any that don't belong to a running process
	entries, err := os.ReadDir("/dev/shm")
	if err != nil {
		return
	}
	prefix := fmt.Sprintf("aeron-%s-", name)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			// Extract PID from directory name
			pidStr := strings.TrimPrefix(entry.Name(), prefix)
			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}
			// Check if this PID is still alive
			if err := syscall.Kill(pid, 0); err != nil {
				// Process is dead, clean up
				dirPath := filepath.Join("/dev/shm", entry.Name())
				os.RemoveAll(dirPath)
				pm.log.Info("cleaned stale gateway aeron dir", "dir", dirPath)
			}
		}
	}
}

// --- Log Management ---

func (pm *ProcessManager) rotateLog(logPath string) {
	if _, err := os.Stat(logPath); err == nil {
		ts := time.Now().Format("20060102-150405")
		os.Rename(logPath, logPath+"."+ts)
	}
}

// --- Metrics Polling ---

func (pm *ProcessManager) backgroundPoller() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.refreshAdoptedProcesses()
		case <-pm.stopChan:
			return
		}
	}
}

// refreshAdoptedProcesses is the watchdog for processes re-attached after an
// admin restart (#13): adoptExisting() has no cmd handle, so monitor() cannot
// wait on them — without this, crashes of adopted processes were never
// detected (no auto-restart, no lastError, and the driver→node RestartCascades
// silently disabled — an incident amplifier on 2026-07-02). Death of an
// adopted process now runs the same crash protocol as monitored ones.
func (pm *ProcessManager) refreshAdoptedProcesses() {
	for _, def := range pm.servicesSnapshot() {
		if def.Name == "admin" {
			continue
		}

		proc := pm.proc(def.Name)
		if proc == nil {
			continue
		}
		proc.mu.Lock()
		adopted := proc.running && proc.pid > 0 && proc.cmd == nil && proc.status == "running"
		pid := proc.pid
		proc.mu.Unlock()

		if !adopted || isProcessAlive(pid) {
			continue
		}

		// Re-check under the lock: an explicit stop/restart may have raced us.
		proc.mu.Lock()
		if !proc.running || proc.pid != pid || proc.cmd != nil {
			proc.mu.Unlock()
			continue
		}
		proc.running = false
		proc.pid = 0
		proc.mu.Unlock()
		pm.removePID(def.Name)

		def := def // capture per-iteration copy for the goroutine
		// Run the crash protocol off the poller goroutine — it sleeps
		// RestartSec (and cascades) before restarting.
		go pm.handleCrash(def, proc, pid, "adopted process died (no exit status available)", nil)
	}
}

// --- Info Helpers ---

func (pm *ProcessManager) getInfo(def ServiceDef) ProcessInfo {
	proc := pm.proc(def.Name)
	if proc == nil {
		// A caller holding a pre-reload def for a removed service: report it
		// stopped rather than panic (it is, by the reload's stopped invariant).
		return ProcessInfo{Name: def.Name, Display: def.Display, Role: def.Role, Port: def.Port, Status: "stopped"}
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()

	info := ProcessInfo{
		Name:         def.Name,
		Display:      def.Display,
		Role:         def.Role,
		Port:         def.Port,
		Running:      proc.running,
		PID:          proc.pid,
		RestartCount: proc.restartCount,
		Enabled:      true,
		Status:       proc.status,
		LastError:    proc.lastError,
	}

	if proc.running && proc.pid > 0 {
		info.MemoryBytes = getProcessMemory(proc.pid)
		info.CPUPercent = getProcessCPU(proc.pid)

		if !proc.startedAt.IsZero() {
			info.UptimeMs = time.Since(proc.startedAt).Milliseconds()
			info.StartedAt = proc.startedAt.Format("Mon 2006-01-02 15:04:05 -07")
		}
	}

	return info
}

// servicesSnapshot returns a copy of the current catalog under a read lock.
// Runtime readers (List/Summary/the poller) iterate the copy so they never race
// a live ReloadCatalog swap (Phase 2 profile switch); before ReloadCatalog the
// catalog was immutable-after-construction and read lock-free.
func (pm *ProcessManager) servicesSnapshot() []ServiceDef {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	out := make([]ServiceDef, len(pm.services))
	copy(out, pm.services)
	return out
}

// proc returns the state record for a service, or nil if unknown. All procs-map
// reads go through here (RLock) because ReloadCatalogMembership can re-key the
// map (services added/removed on a driver-mode or topology change). The returned
// pointer stays valid across a re-key — surviving services keep their record and
// per-process state has its own mutex — so holding it after the RLock is safe.
func (pm *ProcessManager) proc(name string) *managedProcess {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.procs[name]
}

// ReloadCatalog atomically swaps the managed-service catalog, e.g. after a
// profile switch rebuilds every ServiceDef's Command/Env. Membership must be
// identical to the current catalog (same service names) — membership-changing
// swaps (the embedded↔external driver-mode boundary, topology changes) go
// through ReloadCatalogMembership, which requires the affected services to be
// stopped. A running process keeps its old launch spec until it is explicitly
// restarted onto the new def.
func (pm *ProcessManager) ReloadCatalog(newServices []ServiceDef) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(newServices) != len(pm.services) {
		return fmt.Errorf("catalog membership changed (%d→%d services); use the membership-aware apply path (services must be stopped)",
			len(pm.services), len(newServices))
	}
	have := make(map[string]bool, len(pm.services))
	for _, d := range pm.services {
		have[d.Name] = true
	}
	for _, d := range newServices {
		if !have[d.Name] {
			return fmt.Errorf("catalog membership changed (new service %q has no proc state); use the membership-aware apply path", d.Name)
		}
	}
	pm.services = newServices
	return nil
}

// ReloadCatalogMembership swaps the catalog INCLUDING membership changes
// (services added/removed — the embedded↔external driver-mode boundary, and
// topology changes). Safe only while every removed service is stopped: a
// stopped service has no monitor goroutine and no in-flight start, so dropping
// its record cannot orphan a running process. Surviving services keep their
// *managedProcess (uptime/restart history preserved); added services get fresh
// records. The re-key happens under the full write lock and every reader goes
// through proc()/findDef()/servicesSnapshot() (RLock), so nothing races it.
func (pm *ProcessManager) ReloadCatalogMembership(newServices []ServiceDef) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	newSet := make(map[string]bool, len(newServices))
	for _, d := range newServices {
		newSet[d.Name] = true
	}
	// Removed services must be stopped — a running process must never lose the
	// record that tracks it.
	for name, proc := range pm.procs {
		if newSet[name] {
			continue
		}
		proc.mu.Lock()
		active := proc.running || proc.starting
		proc.mu.Unlock()
		if active {
			return fmt.Errorf("cannot reload catalog: service %q would be removed but is still running — stop it first", name)
		}
	}

	// Re-key: keep survivors' state, fresh records for added services.
	procs := make(map[string]*managedProcess, len(newServices))
	for _, d := range newServices {
		if p, ok := pm.procs[d.Name]; ok {
			procs[d.Name] = p
		} else {
			procs[d.Name] = &managedProcess{status: "stopped"}
		}
	}
	pm.services = newServices
	pm.procs = procs
	return nil
}

func (pm *ProcessManager) findDef(name string) *ServiceDef {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for i := range pm.services {
		if pm.services[i].Name == name {
			return &pm.services[i]
		}
	}
	return nil
}

func (pm *ProcessManager) findDependents(name string) []string {
	var deps []string
	for _, def := range pm.servicesSnapshot() {
		for _, d := range def.DependsOn {
			if d == name {
				deps = append(deps, def.Name)
				break
			}
		}
	}
	return deps
}

func (pm *ProcessManager) bootOrder() []ServiceDef {
	return pm.servicesSnapshot()
}

func (pm *ProcessManager) shutdownOrder() []ServiceDef {
	forward := pm.bootOrder()
	reversed := make([]ServiceDef, len(forward))
	for i, def := range forward {
		reversed[len(forward)-1-i] = def
	}
	return reversed
}

// --- System Helpers ---

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Signal 0 checks existence without actually sending a signal
	err := syscall.Kill(pid, 0)
	return err == nil
}

// driverPidFileAlive reads <driverDir>.pid — written by launch-driver.sh
// before it execs aeronmd, so the recorded PID IS the driver's — and reports
// whether it names a live process. This is ground truth independent of the
// PM's tracked state, which is exactly what lies in the #42 incident class:
// a duplicate driver start exits 0 (the script is idempotent), reads as an
// instant crash, and leaves the real driver alive but tracked stopped.
func driverPidFileAlive(driverDir string) (int, bool) {
	data, err := os.ReadFile(driverDir + ".pid")
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, isProcessAlive(pid)
}

// driverCncFreshWindow: a cnc.dat modified more recently than this means a live
// media driver is still mapping/writing it. The Aeron driver conductor touches
// cnc.dat continuously; 30s is comfortably longer than its heartbeat, so a stale
// (crashed-driver) dir ages out of the window quickly while a live one never does.
const driverCncFreshWindow = 30 * time.Second

// cncModifiedWithin reports whether <driverDir>/cnc.dat exists and was modified
// within window. Absent/unstattable cnc.dat reads as "not fresh".
func cncModifiedWithin(driverDir string, window time.Duration) (fresh bool, age time.Duration) {
	st, err := os.Stat(filepath.Join(driverDir, "cnc.dat"))
	if err != nil {
		return false, 0
	}
	age = time.Since(st.ModTime())
	return age >= 0 && age < window, age
}

// canDeleteDriverDir is the #42 guard shared by every deleter of a media driver
// dir: deletion needs the tracked state stopped AND no live process holding it.
//
// The <driverDir>.pid file was the ground truth (external-driver mode, where
// launch-driver.sh writes it). But the cluster now also runs EMBEDDED drivers
// (the media driver inside the node JVM: the assets engine always, and the
// matching engine on the light profile) — there is NO external driver process
// and NO pid file, so the pid-file check is vacuous and defaulted to "safe to
// delete". That is how node0's live driver dir was deleted on 2026-07-08 and
// again 2026-07-10 02:47 (the 02:47 deleter is still UNATTRIBUTED — an external
// process is suspected; this guard closes every in-repo deleter regardless).
//
// So: when the pid file is ABSENT, two embedded-mode liveness signals gate the
// delete, and EITHER blocks — the owning NODE process being tracked-running
// (nodeRunning, supplied by the caller that owns the process registry), and a
// freshly-written cnc.dat. The pid-file path stays the primary check when a pid
// file is present (present-but-dead = the legitimate stale external case).
func canDeleteDriverDir(driverDir string, trackedRunning, nodeRunning bool) (ok bool, reason string) {
	if trackedRunning {
		return false, "driver tracked running"
	}
	if _, err := os.Stat(driverDir + ".pid"); err == nil {
		// External-driver mode: the pid file is ground truth.
		if pid, alive := driverPidFileAlive(driverDir); alive {
			return false, fmt.Sprintf("live driver (pid %d) holds it per %s.pid despite tracked stopped", pid, driverDir)
		}
		return true, "" // pid file present but dead: the legitimate stale case
	}
	// No pid file: embedded-driver mode. The pid-file guard cannot fire, so fall
	// back to the node process and a live cnc.dat; either blocks the delete.
	if nodeRunning {
		return false, "owning node process is tracked running and there is no driver pid file (embedded driver)"
	}
	if fresh, age := cncModifiedWithin(driverDir, driverCncFreshWindow); fresh {
		return false, fmt.Sprintf("cnc.dat modified %v ago (< %v): a live embedded driver is mapping it",
			age.Truncate(time.Millisecond), driverCncFreshWindow)
	}
	return true, ""
}

func getProcessMemory(pid int) int64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	rss, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return rss * 4096
}

func getProcessCPU(pid int) float64 {
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu", "--no-headers")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return val
}

func getProcessStartTime(pid int) time.Time {
	// Read /proc/[pid]/stat for start time
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return time.Time{}
	}

	// Field 22 is starttime (in clock ticks since boot)
	// Find the closing paren of the comm field first
	s := string(data)
	idx := strings.LastIndex(s, ")")
	if idx < 0 {
		return time.Time{}
	}
	fields := strings.Fields(s[idx+2:]) // skip ") "
	if len(fields) < 20 {
		return time.Time{}
	}

	startTicks, err := strconv.ParseInt(fields[19], 10, 64) // field 22 - 3 = index 19
	if err != nil {
		return time.Time{}
	}

	// Get system boot time
	bootTime := getBootTime()
	if bootTime.IsZero() {
		return time.Time{}
	}

	clkTck := int64(100) // sysconf(_SC_CLK_TCK) = 100 on Linux
	startSec := startTicks / clkTck
	startNsec := (startTicks % clkTck) * (1e9 / clkTck)

	return bootTime.Add(time.Duration(startSec)*time.Second + time.Duration(startNsec)*time.Nanosecond)
}

var cachedBootTime time.Time

func getBootTime() time.Time {
	if !cachedBootTime.IsZero() {
		return cachedBootTime
	}

	f, err := os.Open("/proc/stat")
	if err != nil {
		return time.Time{}
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "btime ") {
			ts, err := strconv.ParseInt(strings.TrimPrefix(line, "btime "), 10, 64)
			if err != nil {
				return time.Time{}
			}
			cachedBootTime = time.Unix(ts, 0)
			return cachedBootTime
		}
	}
	return time.Time{}
}

// Ensure unused imports are consumed
var _ = bufio.NewReader
var _ io.Reader
