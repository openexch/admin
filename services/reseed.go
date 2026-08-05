// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Stranded-member reseed (match#35 → #12 / P3.2).
//
// When a member's archive is corrupt (replay hot-loop, "does not point to a
// valid frame", rejoin loops), restarts do not fix it: the validated recovery
// is to copy a healthy follower's cluster/ + archive/ dirs over the stranded
// member's wiped state, excluding the per-member identity files, then start
// both. This automates that manual procedure exactly.
//
// COST: the source follower is stopped for the copy, so with the target
// already dead the cluster runs on the leader alone (quorum lost, ingress
// stalls) for the seconds the copy takes. That is why force=true is required.

// reseedExcluded: per-member identity files that must NEVER be copied or
// wiped (the member keeps its own identity; *.lck are live-process locks).
func reseedExcluded(name string) bool {
	if strings.HasPrefix(name, "cluster-mark") && strings.HasSuffix(name, ".dat") {
		return true
	}
	return name == "node-state.dat" || name == "archive-mark.dat" ||
		strings.HasSuffix(name, ".lck")
}

// ReseedNode validates and launches the reseed operation.
func (o *OperationsService) ReseedNode(targetId, sourceId int, force bool) error {
	if targetId < 0 || targetId >= o.cluster.NodeCount() || sourceId < 0 || sourceId >= o.cluster.NodeCount() {
		return fmt.Errorf("nodeId and sourceNodeId must be 0..%d", o.cluster.NodeCount()-1)
	}
	if targetId == sourceId {
		return fmt.Errorf("sourceNodeId must differ from nodeId")
	}
	if !force {
		return fmt.Errorf("reseed stops the healthy source follower too — the cluster loses " +
			"quorum for the seconds the copy takes; confirm with force=true")
	}
	leader := o.cluster.DetectLeader()
	if leader == sourceId {
		return fmt.Errorf("node%d is the LEADER — reseed from a healthy FOLLOWER", sourceId)
	}
	if leader == targetId {
		return fmt.Errorf("node%d is currently the LEADER — a leader is not stranded; refusing", targetId)
	}
	if !o.progress.TryStart("reseed-node", 7) {
		return fmt.Errorf("another operation in progress")
	}
	go o.doReseedNode(targetId, sourceId, leader)
	return nil
}

func (o *OperationsService) doReseedNode(targetId, sourceId, leader int) {
	defer o.recoverOp() // ag#67: contain+record a panic, free the slot
	log := o.log.With("op", "reseed-node", "op_id", o.progress.CurrentOpID(),
		"target", targetId, "source", sourceId)
	targetDir := o.cluster.NodeStateDir(targetId)
	sourceDir := o.cluster.NodeStateDir(sourceId)

	// Step 0: will it even fit? Check BEFORE anything is stopped. Running out of
	// room mid-copy is what turns a reseed into a quorum outage: the destination
	// fills and the healthy members can no longer allocate their own log buffers.
	if err := checkReseedSpace(sourceDir, targetDir); err != nil {
		log.Error("reseed refused on space", "err", err)
		o.progress.Finish(false, "Reseed refused: "+err.Error())
		return
	}

	// Step 1: stop the stranded target (force: it may be wedged or crash-looping)
	o.progress.Update(1, fmt.Sprintf("Force-stopping node%d (stranded target)...", targetId))
	o.clusterStatus.SetNodeStatus(targetId, "STOPPING", false)
	if err := o.procMgr.ForceStop(o.cluster.NodeName(targetId)); err != nil {
		log.Warn("force-stop of target reported an error (may already be dead)", "err", err)
	}
	o.waitForNodeStopped(log, targetId, 15*time.Second)
	o.clusterStatus.SetNodeStatus(targetId, "OFFLINE", false)

	// Step 2: stop the healthy source follower (quorum outage begins)
	o.progress.Update(2, fmt.Sprintf("Stopping node%d (healthy source) — quorum outage begins...", sourceId))
	o.clusterStatus.SetNodeStatus(sourceId, "STOPPING", false)
	o.stopService(o.cluster.NodeName(sourceId))
	o.waitForNodeStopped(log, sourceId, 15*time.Second)
	o.clusterStatus.SetNodeStatus(sourceId, "OFFLINE", false)

	// Restart the source no matter what happens past this point: it is the
	// healthy member, and leaving it down turns a failed reseed into a
	// quorum-loss incident.
	restartSource := func() {
		o.progress.Update(6, fmt.Sprintf("Starting node%d (source)...", sourceId))
		o.clusterStatus.SetNodeStatus(sourceId, "STARTING", true)
		o.startService(o.cluster.NodeName(sourceId))
	}

	// Step 3: wipe the target's state, keeping its identity files
	o.progress.Update(3, fmt.Sprintf("Wiping node%d state (keeping identity files)...", targetId))
	if err := wipeStateDirs(targetDir); err != nil {
		restartSource()
		o.progress.Finish(false, fmt.Sprintf("Wipe of node%d state failed: %v — source node%d restarted, "+
			"target left stopped", targetId, err, sourceId))
		return
	}

	// Step 4: copy the source's cluster/ + archive/ over
	o.progress.Update(4, fmt.Sprintf("Copying node%d state over node%d...", sourceId, targetId))
	copied, err := copyStateDirs(sourceDir, targetDir)
	if err != nil {
		restartSource()
		o.progress.Finish(false, fmt.Sprintf("State copy failed: %v — source node%d restarted, "+
			"target left stopped (re-run reseed)", err, sourceId))
		return
	}
	log.Info("state copied", "files", copied)

	// Step 5/6: start source first (ends the quorum outage), then target
	restartSource()
	o.progress.Update(5, fmt.Sprintf("Waiting for node%d (source) to rejoin...", sourceId))
	if !o.waitForFollowerCatchUp(log, sourceId, leader, 60*time.Second) {
		o.progress.Finish(false, fmt.Sprintf("Source node%d did not confirm catch-up within 60s — "+
			"target node%d NOT started; verify the cluster before starting it", sourceId, targetId))
		return
	}
	o.clusterStatus.SetNodeStatus(sourceId, "FOLLOWER", true)

	o.progress.Update(6, fmt.Sprintf("Starting node%d (reseeded target)...", targetId))
	o.clusterStatus.SetNodeStatus(targetId, "STARTING", true)
	o.startService(o.cluster.NodeName(targetId))

	// Step 7: wait for the reseeded member to catch up
	o.progress.Update(7, fmt.Sprintf("Waiting for node%d to catch up...", targetId))
	if !o.waitForFollowerCatchUp(log, targetId, leader, 120*time.Second) {
		o.progress.Finish(false, fmt.Sprintf("node%d was reseeded and started but did not confirm "+
			"catch-up within 120s — check its logs (a second failure means the source copy is bad)", targetId))
		return
	}
	o.clusterStatus.SetNodeStatus(targetId, "FOLLOWER", true)
	log.Info("reseed complete", "files", copied)
	o.progress.Finish(true, fmt.Sprintf("node%d reseeded from node%d (%d files) and caught up to the leader",
		targetId, sourceId, copied))
}

// wipeStateDirs removes everything under <node>/cluster and <node>/archive
// except the identity files.
func wipeStateDirs(nodeDir string) error {
	for _, sub := range []string{"cluster", "archive"} {
		dir := filepath.Join(nodeDir, sub)
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			if reseedExcluded(e.Name()) {
				continue
			}
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// sparseCopyBlock is the granularity at which an all-zero run becomes a hole.
const sparseCopyBlock = 128 * 1024

// copyFileSparse copies preserving HOLES.
//
// Aeron pre-allocates its archive segments (a 64MB segment file may hold a few
// MB of frames), so the files are sparse: `du` reports the allocated blocks,
// `ls` the apparent size. A plain io.Copy reads the holes as zeros and writes
// them out as real blocks, inflating the destination to the apparent size.
//
// On 2026-07-25 that turned a ~193MB reseed into 4.4GB written into /dev/shm,
// filled tmpfs, and left the healthy source node unable to allocate its own
// log buffer — the matching engine lost quorum for ~20 minutes. Same class as
// the sparse-copy gotcha already known from the 2026-07-13 boundaries review.
func copyFileSparse(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, sparseCopyBlock)
	var offset int64
	for {
		n, readErr := in.Read(buf)
		if n > 0 {
			// An all-zero run is left as a hole: WriteAt is simply skipped, and
			// the final Truncate gives the file its full logical size.
			if !isAllZero(buf[:n]) {
				if _, err := out.WriteAt(buf[:n], offset); err != nil {
					return err
				}
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	// Preserve the logical size even when the file ends in a hole.
	return out.Truncate(info.Size())
}

func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// allocatedBytes is what a copy will ACTUALLY consume (blocks in use), not the
// apparent size. This is the number to check free space against.
func allocatedBytes(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || reseedExcluded(info.Name()) {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512 // st_blocks is always in 512-byte units
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// freeBytes reports the usable space on the filesystem holding path.
func freeBytes(path string) (int64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, err
	}
	return int64(fs.Bavail) * int64(fs.Bsize), nil
}

// checkReseedSpace refuses a reseed that cannot fit, BEFORE the source follower
// is stopped. Discovering this mid-copy is how a reseed turns into a quorum
// outage: the destination fills, and the healthy nodes can no longer allocate
// their own log buffers. Requires a 25% margin because the cluster keeps
// writing while the copy runs.
func checkReseedSpace(srcNodeDir, dstNodeDir string) error {
	var need int64
	for _, sub := range []string{"cluster", "archive"} {
		root := filepath.Join(srcNodeDir, sub)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		}
		n, err := allocatedBytes(root)
		if err != nil {
			return fmt.Errorf("could not size %s: %w", root, err)
		}
		need += n
	}
	free, err := freeBytes(dstNodeDir)
	if err != nil {
		return fmt.Errorf("could not check free space on %s: %w", dstNodeDir, err)
	}
	required := need + need/4
	if free < required {
		return fmt.Errorf("reseed needs ~%d MB (%d MB of state plus a 25%% margin) but only %d MB "+
			"is free on %s — free space first; copying anyway would fill the filesystem and "+
			"cost the healthy nodes their quorum",
			required/(1<<20), need/(1<<20), free/(1<<20), dstNodeDir)
	}
	return nil
}

// copyStateDirs recursively copies <src>/cluster and <src>/archive into
// <dst>, skipping the identity files, and returns the file count.
func copyStateDirs(srcNodeDir, dstNodeDir string) (int, error) {
	copied := 0
	for _, sub := range []string{"cluster", "archive"} {
		srcRoot := filepath.Join(srcNodeDir, sub)
		if _, err := os.Stat(srcRoot); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if reseedExcluded(info.Name()) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(srcRoot, path)
			if err != nil {
				return err
			}
			dst := filepath.Join(dstNodeDir, sub, rel)
			if info.IsDir() {
				return os.MkdirAll(dst, 0755)
			}
			if err := copyFileSparse(path, dst); err != nil {
				return err
			}
			copied++
			return nil
		})
		if err != nil {
			return copied, err
		}
	}
	return copied, nil
}
