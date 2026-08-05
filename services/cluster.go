// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/openexch/admin/config"
)

// Stock Aeron tooling main classes — identical for every Aeron cluster.
const (
	clusterToolMain = "io.aeron.cluster.ClusterTool"
	archiveToolMain = "io.aeron.archive.ArchiveTool"
)

// Archive reclamation is ONE class now, shared by both engines via cluster-kit
// and shaded into each jar. It reads a recording log and purges log segments;
// nothing in it knows what the state machine means, so there was never a reason
// for two copies — and the copies had already drifted.
//
// Bundle capture stays per engine, but only as a ~15-line entry point supplying
// the one fact the machinery cannot derive: which wire schema that engine
// speaks. The 500 lines behind it are shared.
const clusterKitHousekeeping = "com.openexchange.cluster.ArchiveHousekeeping"

// Settlement-journal retention main class (match-cluster) + its archive control
// port offset. The offset mirrors match-common
// InfrastructureConstants.JOURNAL_ARCHIVE_CONTROL_PORT_OFFSET: the journal
// archive's control channel sits at PortBase + node*100 + 10, distinct from the
// consensus archive's +1 offset.
const (
	journalRetentionMain            = "com.match.infrastructure.journal.JournalRetention"
	journalArchiveControlPortOffset = 10
)

// Cluster is the descriptor + tooling wrapper for ONE Aeron cluster. Every
// per-cluster identity (node count, ports, dirs, jar, driver topology, tooling)
// lives here, so a single set of management operations (rolling update, restart,
// snapshot, status, counters, cleanup) serves both the matching engine and the
// assets engine — the only difference is which descriptor they run against.
//
// The media driver is modeled as PART OF the cluster: external clusters own a
// driver process per node (coupled to the node in ServiceDefs), embedded clusters
// run the driver inside the node JVM. Either way the driver's aeron.dir / CnC path
// is derived here (CncPath), so counters and housekeeping resolve it uniformly.
type Cluster struct {
	cfg *config.Config

	Name    string // stable id: "match" | "assets"
	Display string // human label
	Kind    string // UI discriminator: "match" | "assets" (icon/money-slot branch)

	// nodeCount is MUTABLE: the topology-change op (genesis re-form) rewrites it
	// on the live descriptor while status pollers read it concurrently, so all
	// access goes through NodeCount()/SetNodeCount (atomic). Boot value = the
	// cluster-topology.json entry, falling back to the constructor default
	// (match=3, assets=1). Valid counts are odd (Raft majority): 1/3/5/7.
	nodeCount atomic.Int32

	// Cached state-dir usage: computed in-process (see stateDirUsage), never by
	// spawning du. Short TTL so a fast-polling console does not re-walk on
	// every request.
	usageMu    sync.Mutex
	usageCache map[int]stateDirUsage

	PortBase int    // 9000 | 9300; ingress = PortBase + node*100 + 2
	StateDir string // cluster+archive state root: cfg.ClusterDir (/dev/shm/aeron-cluster) | cfg.AssetsStateDir (default /dev/shm/aeron-assets, configurable via ASSETS_STATE_DIR)

	NodePrefix   string // node service + state-dir name prefix: "node" -> node0, "ae" -> ae0
	DriverPrefix string // external driver service prefix: "driver" -> driver0; "" when embedded

	NodeDisplay   string // node card label prefix: "Cluster Node" -> "Cluster Node 0"
	DriverDisplay string // driver card label prefix: "Media Driver" -> "Media Driver 0"

	// Embedded = the media driver runs inside the node JVM (no separate driver
	// service). External = one driver process per node, coupled in ServiceDefs.
	Embedded bool

	// DriverDirInfix distinguishes the media driver's aeron.dir between clusters
	// so their CnC files never collide: "" -> aeron-<user>-<n>-driver (match),
	// "assets-" -> aeron-<user>-assets-<n>-driver (assets embedded driver).
	DriverDirInfix string

	Jar string // launch/tooling classpath: match-cluster.jar | assets-cluster.jar

	// ProjectDir is the checkout the cluster's processes are launched from
	// (their working directory): cfg.ProjectDir | cfg.AssetsProjectDir.
	ProjectDir string

	// HousekeepingMain is the cluster's live archive-reclaim main class; "" skips
	// housekeeping (e.g. the assets engine has none yet).
	HousekeepingMain string

	// BackupDir is the disk backup location; "" = the cluster has no backup service.
	BackupDir string

	// RichArchiveStats gates the one remaining per-poll JVM spawn: DetectLeader,
	// which runs `ClusterTool list-members` and is authoritative across terms and
	// transitional states. False for a lean cluster on the shared box, which
	// derives its leader from the CnC role counter instead, so the ME's busy-spin
	// cores are never starved by a second cluster's polling.
	//
	// It used to gate the archive sizes (#106) and the log/snapshot positions too.
	// Both are now computed in-process — a directory walk and a 64-byte-record
	// file parse — so neither needs a guard, and neither is a blind spot on the
	// clusters that were "lean". Hiding the Assets Engine's disk use and restore
	// point behind this flag is how it filled /dev/shm unseen on 2026-07-25.
	RichArchiveStats bool
}

// NodeCount is the cluster's live member count (atomic — see the field doc).
func (c *Cluster) NodeCount() int { return int(c.nodeCount.Load()) }

// SetNodeCount commits a topology change on the live descriptor. Callers (the
// topology op only) persist it via config.PersistTopology and re-form the
// cluster from genesis — Aeron 1.51 membership is static.
func (c *Cluster) SetNodeCount(n int) { c.nodeCount.Store(int32(n)) }

// Capabilities reports what management operations this cluster supports, derived
// from its descriptor. The UI gates buttons on these (no Housekeeping/Backup for a
// cluster that has neither), and the handlers reject incapable ops before touching
// the shared operation slot.
func (c *Cluster) Capabilities() map[string]interface{} {
	return map[string]interface{}{
		"snapshot":       true, // every Aeron cluster: ClusterTool snapshot
		"cleanup":        true, // every Aeron cluster: stale /dev/shm reclaim
		"housekeeping":   c.HousekeepingMain != "",
		"backup":         c.BackupDir != "",
		"separateDriver": !c.Embedded,
	}
}

// NewMatchCluster is the matching-engine descriptor — exactly today's values, so
// migrating the ops onto it is behavior-preserving. Node count comes from the
// topology store (default 3).
func NewMatchCluster(cfg *config.Config) *Cluster {
	c := &Cluster{
		cfg:              cfg,
		Name:             "match",
		Display:          "Matching Engine",
		Kind:             "match",
		PortBase:         9000,
		StateDir:         cfg.ClusterDir, // /dev/shm/aeron-cluster
		NodePrefix:       "node",
		DriverPrefix:     "driver",
		NodeDisplay:      "Cluster Node",
		DriverDisplay:    "Media Driver",
		Embedded:         false,
		DriverDirInfix:   "",
		Jar:              cfg.JarPath,
		ProjectDir:       cfg.ProjectDir,
		HousekeepingMain: clusterKitHousekeeping,
		BackupDir:        filepath.Join(cfg.ProjectDir, "backup"),
		RichArchiveStats: true, // the ME status has always carried positions/archive sizes
	}
	c.nodeCount.Store(int32(cfg.NodeCountFor(c.Name, 3)))
	return c
}

// NewAssetsCluster is the money-ledger descriptor: embedded-driver nodes on
// disjoint ports/dirs. Node count comes from the topology store (default 1) —
// flip to a multi-node cluster with no code change. StateDir comes from
// cfg.AssetsStateDir (env ASSETS_STATE_DIR, default /dev/shm/aeron-assets):
// an operator moving the money ledger off tmpfs onto real disk changes only
// that env var; see the ae-state-on-disk preflight check.
func NewAssetsCluster(cfg *config.Config) *Cluster {
	c := &Cluster{
		cfg:              cfg,
		Name:             "assets",
		Display:          "Assets Engine",
		Kind:             "assets",
		PortBase:         9300,
		StateDir:         cfg.AssetsStateDir,
		NodePrefix:       "ae",
		DriverPrefix:     "", // embedded: no external driver service
		NodeDisplay:      "Assets Engine",
		DriverDisplay:    "",
		Embedded:         true,
		DriverDirInfix:   "assets-",
		Jar:              cfg.AssetsJar,
		ProjectDir:       cfg.AssetsProjectDir,
		HousekeepingMain: clusterKitHousekeeping,
		BackupDir:        "",    // no backup yet
		RichArchiveStats: false, // CnC-counter-only status: no JVM/du on the shared box
	}
	c.nodeCount.Store(int32(cfg.NodeCountFor(c.Name, 1)))
	return c
}

// NewCluster preserves the original single-cluster constructor (the matching
// engine) for callers not yet cluster-aware.
func NewCluster(cfg *config.Config) *Cluster {
	return NewMatchCluster(cfg)
}

// ---- per-cluster naming / paths (the seam every operation reads from) ----

// NodeName is the process-manager service name for node i (node0 / ae0).
func (c *Cluster) NodeName(i int) string { return fmt.Sprintf("%s%d", c.NodePrefix, i) }

// DriverName is the external driver service name for node i (driver0); empty when embedded.
func (c *Cluster) DriverName(i int) string {
	if c.Embedded {
		return ""
	}
	return fmt.Sprintf("%s%d", c.DriverPrefix, i)
}

// NodeStateDir is the cluster+archive state root for node i.
func (c *Cluster) NodeStateDir(i int) string {
	return fmt.Sprintf("%s/%s", c.StateDir, c.NodeName(i))
}

func (c *Cluster) clusterSubDir(i int) string { return c.NodeStateDir(i) + "/cluster" }
func (c *Cluster) archiveSubDir(i int) string { return c.NodeStateDir(i) + "/archive" }

// IngressPort is the client-facing port for node i (PortBase + node*100 + 2).
func (c *Cluster) IngressPort(i int) int { return c.PortBase + i*100 + 2 }

// DriverAeronDir is the aeron.dir of node i's media driver (external process or
// embedded) — the single source of truth for its CnC location.
func (c *Cluster) DriverAeronDir(i int) string {
	return fmt.Sprintf("/dev/shm/aeron-%s-%s%d-driver", currentUsername(), c.DriverDirInfix, i)
}

// CncPath is node i's Aeron CnC file (the counters source).
func (c *Cluster) CncPath(i int) string { return c.DriverAeronDir(i) + "/cnc.dat" }

// NodeServiceDefs generates this cluster's process-manager service definitions:
// its nodes and, in external-driver mode, one coupled media-driver process per
// node. The media driver is thus PART of the cluster — generated here with the
// driver↔node linkage (DependsOn / GatedBy / WaitForFile / RestartCascades /
// DriverDir) DERIVED from the descriptor, never hand-wired. The per-node launch
// specifics (which are profile-driven for the matching engine and fixed for the
// assets engine) are supplied by the caller's closures.
//
// external is passed explicitly because a cluster's driver topology can be
// profile-dependent (the matching engine runs external drivers on the demo/perf
// profiles but an embedded driver on the light profile), independent of the
// descriptor's static Embedded flag.
func (c *Cluster) NodeServiceDefs(
	external bool,
	nodeCmd func(i int) []string,
	driverCmd func(i int) []string,
	nodeEnv func(i int) map[string]string,
	preStart func(i int) [][]string,
) []ServiceDef {
	defs := make([]ServiceDef, 0, c.NodeCount()*2)

	// Driver services first (boot order): each coupled to its node.
	if external {
		for i := 0; i < c.NodeCount(); i++ {
			defs = append(defs, ServiceDef{
				Name:            c.DriverName(i),
				Display:         fmt.Sprintf("%s %d", c.DriverDisplay, i),
				Role:            RoleInfra,
				Command:         driverCmd(i),
				WorkDir:         c.ProjectDir,
				AutoRestart:     true,
				RestartSec:      3,
				StopTimeout:     5,
				RestartCascades: []string{c.NodeName(i)}, // a node cannot outlive its driver's shm files
				DriverDir:       c.DriverAeronDir(i),
			})
		}
	}

	// Node services, gated on their driver in external mode.
	for i := 0; i < c.NodeCount(); i++ {
		def := ServiceDef{
			Name:        c.NodeName(i),
			Display:     fmt.Sprintf("%s %d", c.NodeDisplay, i),
			Role:        RoleClusterNode,
			Command:     nodeCmd(i),
			Env:         nodeEnv(i),
			WorkDir:     c.ProjectDir,
			PreStart:    preStart(i),
			AutoRestart: true,
			RestartSec:  10,
			StopTimeout: 5,
			Artifact:    c.Jar,
		}
		if external {
			def.DependsOn = []string{c.DriverName(i)}
			def.WaitForFile = filepath.Join(c.DriverAeronDir(i), "cnc.dat")
			def.GatedBy = c.DriverName(i)
		}
		defs = append(defs, def)
	}
	return defs
}

// ---- ClusterTool / ArchiveTool (stock Aeron; only clusterDir + jar differ) ----

// clusterTool runs a ClusterTool command for a specific node
func (c *Cluster) clusterTool(nodeId int, command string) (string, error) {
	cmd := exec.Command("java",
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", c.Jar,
		clusterToolMain,
		c.clusterSubDir(nodeId), command)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// archiveTool runs an ArchiveTool command for a specific node
func (c *Cluster) archiveTool(nodeId int, command string) (string, error) {
	cmd := exec.Command("java",
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", c.Jar,
		archiveToolMain,
		c.archiveSubDir(nodeId), command)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// DetectLeader finds the current cluster leader
func (c *Cluster) DetectLeader() int {
	for nodeId := 0; nodeId < c.NodeCount(); nodeId++ {
		output, err := c.clusterTool(nodeId, "list-members")
		if err != nil {
			continue
		}
		// Parse leaderMemberId=N from output
		re := regexp.MustCompile(`leaderMemberId=(\d+)`)
		matches := re.FindStringSubmatch(output)
		if len(matches) > 1 {
			leader, _ := strconv.Atoi(matches[1])
			return leader
		}
	}
	return -1
}

// GetRecordingLog returns the recording log for a node
func (c *Cluster) GetRecordingLog(nodeId int) (string, error) {
	return c.clusterTool(nodeId, "recording-log")
}

// TakeSnapshot triggers a snapshot on the leader
func (c *Cluster) TakeSnapshot(leaderNode int) (string, error) {
	return c.clusterTool(leaderNode, "snapshot")
}

// GetLogAndSnapshotPositions returns the node's newest log position and newest
// snapshot position, read straight from its recording.log.
//
// This used to shell out to `ClusterTool recording-log` and regex the printed
// text — a JVM per node per poll, which is why these numbers were gated behind
// RichArchiveStats and the Assets Engine showed none of them. Parsing the file
// in-process costs microseconds, so every cluster reports its restore point and
// the matching engine loses three JVM spawns per poll. See recordinglog.go.
func (c *Cluster) GetLogAndSnapshotPositions(nodeId int) (logPos int64, snapPos int64) {
	entries, err := ReadRecordingLog(c.clusterSubDir(nodeId))
	if err != nil {
		return -1, -1
	}
	return LatestPositions(entries)
}

// GetSnapshotPosition is the node's newest valid snapshot position, or -1.
func (c *Cluster) GetSnapshotPosition(nodeId int) int64 {
	_, snapPos := c.GetLogAndSnapshotPositions(nodeId)
	return snapPos
}

// GetArchiveSize returns the archive size in bytes for a node
func (c *Cluster) GetArchiveSize(nodeId int) int64 {
	u, ok := c.stateDirUsage(nodeId)
	if !ok {
		return -1
	}
	return u.apparent
}

// GetArchiveDiskUsage returns actual disk usage in bytes
func (c *Cluster) GetArchiveDiskUsage(nodeId int) int64 {
	u, ok := c.stateDirUsage(nodeId)
	if !ok {
		return -1
	}
	return u.allocated
}

// stateDirUsage is a node's on-disk footprint, computed IN-PROCESS.
//
// This used to shell out to `du` twice per node per poll, which is why the
// assets cluster was built with RichArchiveStats=false and therefore never
// reported its size at all. That is how the Assets Engine grew to 1.2GB per
// node in tmpfs, filled /dev/shm and took the money path down on 2026-07-25
// without the console ever showing the number that mattered. A directory walk
// costs microseconds and no process spawn, so every cluster can afford it.
//
// allocated = blocks actually in use (what fills the filesystem).
// apparent  = logical size; Aeron pre-allocates sparse segments, so the two
// differ by a lot and only `allocated` answers "will we run out of room".
func (c *Cluster) stateDirUsage(nodeId int) (stateDirUsage, bool) {
	const ttl = 10 * time.Second

	c.usageMu.Lock()
	if u, ok := c.usageCache[nodeId]; ok && time.Since(u.at) < ttl {
		c.usageMu.Unlock()
		return u, true
	}
	c.usageMu.Unlock()

	root := c.NodeStateDir(nodeId)
	var allocated, apparent int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		apparent += info.Size()
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			allocated += st.Blocks * 512 // st_blocks is always 512-byte units
		} else {
			allocated += info.Size()
		}
		return nil
	})
	if err != nil {
		return stateDirUsage{}, false
	}

	u := stateDirUsage{allocated: allocated, apparent: apparent, at: time.Now()}
	c.usageMu.Lock()
	if c.usageCache == nil {
		c.usageCache = make(map[int]stateDirUsage)
	}
	c.usageCache[nodeId] = u
	c.usageMu.Unlock()
	return u, true
}

type stateDirUsage struct {
	allocated int64
	apparent  int64
	at        time.Time
}

// SeedRecordingLogFromSnapshot resets a node's recording-log from its latest snapshot
func (c *Cluster) SeedRecordingLogFromSnapshot(nodeId int) (string, error) {
	return c.clusterTool(nodeId, "seed-recording-log-from-snapshot")
}

// ArchiveHousekeeping reclaims archive disk on a LIVE node (no downtime):
// it purges whole log segment files below the latest snapshot position. This
// is the ONLY safe in-place reclaimer — it never runs Aeron's offline
// ArchiveTool against a running node (doing so corrupts snapshot recordings
// and breaks recover-from-snapshot). Invoked automatically after every snapshot.
//
// Clusters without a housekeeping main class (HousekeepingMain == "") skip it.
func (c *Cluster) ArchiveHousekeeping(nodeId int) (string, error) {
	return c.ArchiveHousekeepingTo(nodeId, -1)
}

// ArchiveHousekeepingTo purges below a retention watermark. A negative watermark
// means "no external constraint" and reproduces the snapshot-only purge.
//
// The watermark travels as a NAMED flag. The trailing "2" below is a long dead
// snapshotsToKeep the tool ignores, and it parses perfectly well as a log
// position: passed positionally it would mean "purge nothing", the log would
// grow unbounded and the disk would fill. That is the 2026-07-25 outage rebuilt
// by an argument nobody changed.
func (c *Cluster) ArchiveHousekeepingTo(nodeId int, watermark int64) (string, error) {
	if c.HousekeepingMain == "" {
		return fmt.Sprintf("housekeeping skipped: %s has no housekeeping tool", c.Name), nil
	}
	args := []string{
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", c.Jar,
		c.HousekeepingMain,
		c.clusterSubDir(nodeId), c.DriverAeronDir(nodeId), "2",
	}
	if watermark >= 0 {
		args = append(args, fmt.Sprintf("--watermark=%d", watermark))
	}

	output, err := exec.Command("java", args...).CombinedOutput()
	return string(output), err
}

// ArchiveControlPort is the node archive's UDP control channel port. The capture
// tool dials it from its own isolated archive; the live nodes need no change.
func (c *Cluster) ArchiveControlPort(nodeId int) int {
	return c.PortBase + nodeId*100 + 1
}

// JournalRetention reclaims disk on a node's LIVE settlement-journal archive by
// purging journal segments strictly below the safeEgressSeq watermark. It execs
// the match-cluster JournalRetention CLI — live-safe, no downtime — mirroring
// ArchiveHousekeeping's per-node java invocation. journalRoot is the settlement
// journal root (config.SettlementJournalDir); the per-node dir is
// journalRoot/node<N>, and the journal archive's control port is
// PortBase + N*100 + journalArchiveControlPortOffset.
//
// TODO(settlement-bridge): the safeEgressSeq is the Assets Engine's
// applied-settlement watermark. This method is operator-driven for now; when the
// settlement bridge lands, an auto-hook will call it after each AE snapshot.
func (c *Cluster) JournalRetention(nodeId int, journalRoot string, safeEgressSeq int64) (string, error) {
	journalNodeDir := filepath.Join(journalRoot, fmt.Sprintf("node%d", nodeId))
	controlPort := c.PortBase + nodeId*100 + journalArchiveControlPortOffset
	cmd := exec.Command("java",
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", c.Jar,
		journalRetentionMain,
		journalNodeDir,
		c.DriverAeronDir(nodeId),
		strconv.Itoa(controlPort),
		strconv.FormatInt(safeEgressSeq, 10))

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// currentUsername returns the OS user, matching Aeron's default
// CommonContext directory naming (/dev/shm/aeron-<user>-<node>-driver).
func currentUsername() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "unknown"
}

// driverDirPath is the single source of truth for an EXTERNAL matching-engine
// media driver's aeron.dir. Every reader and (guarded) deleter of these dirs must
// derive the path here so the #42 protections cannot diverge from the launch
// config. (Equivalent to NewMatchCluster(cfg).DriverAeronDir(nodeId).)
func driverDirPath(nodeId int) string {
	return fmt.Sprintf("/dev/shm/aeron-%s-%d-driver", currentUsername(), nodeId)
}

// ArchiveToolDescribe describes the archive catalog for a node
func (c *Cluster) ArchiveToolDescribe(nodeId int) (string, error) {
	cmd := exec.Command("java",
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", c.Jar,
		archiveToolMain,
		c.archiveSubDir(nodeId), "describe")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// GetRecordingLogRecordingIds parses recording IDs from the recording-log
func (c *Cluster) GetRecordingLogRecordingIds(nodeId int) []int64 {
	output, err := c.clusterTool(nodeId, "recording-log")
	if err != nil {
		return nil
	}
	return extractRecordingIds(output)
}

// GetArchiveCatalogRecordingIds parses recording IDs from the archive catalog
func (c *Cluster) GetArchiveCatalogRecordingIds(nodeId int) []int64 {
	output, err := c.ArchiveToolDescribe(nodeId)
	if err != nil {
		return nil
	}
	return extractRecordingIds(output)
}

// extractRecordingIds parses recordingId=N from tool output
func extractRecordingIds(output string) []int64 {
	re := regexp.MustCompile(`recordingId=(\d+)`)
	matches := re.FindAllStringSubmatch(output, -1)
	ids := make([]int64, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			id, _ := strconv.ParseInt(match[1], 10, 64)
			ids = append(ids, id)
		}
	}
	return ids
}
