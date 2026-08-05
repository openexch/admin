// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"path/filepath"
	"testing"
)

func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(filepath.Base(path)), 0644); err != nil {
		t.Fatal(err)
	}
}

// The wipe and copy must both preserve the target's identity files and never
// transplant the source's — that is the whole trick of the validated manual
// procedure (a member keeps its own identity, gets someone else's state).
func TestReseedWipeAndCopyRespectIdentityFiles(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Source: real state + its OWN identity files (must not be copied).
	touch(t, filepath.Join(src, "cluster/recording.log"))
	touch(t, filepath.Join(src, "cluster/cluster-mark.dat"))
	touch(t, filepath.Join(src, "cluster/cluster-mark-service-0.dat"))
	touch(t, filepath.Join(src, "cluster/node-state.dat"))
	touch(t, filepath.Join(src, "archive/archive.catalog"))
	touch(t, filepath.Join(src, "archive/archive-mark.dat"))
	touch(t, filepath.Join(src, "archive/0-1.rec"))
	touch(t, filepath.Join(src, "archive/sub/seg.log"))

	// Target: corrupt state + its own identity files (must survive the wipe).
	touch(t, filepath.Join(dst, "cluster/recording.log"))
	touch(t, filepath.Join(dst, "cluster/cluster-mark.dat"))
	touch(t, filepath.Join(dst, "cluster/node-state.dat"))
	touch(t, filepath.Join(dst, "archive/archive-mark.dat"))
	touch(t, filepath.Join(dst, "archive/archive.catalog"))
	touch(t, filepath.Join(dst, "archive/stale.lck"))

	if err := wipeStateDirs(dst); err != nil {
		t.Fatal(err)
	}
	for _, kept := range []string{"cluster/cluster-mark.dat", "cluster/node-state.dat", "archive/archive-mark.dat", "archive/stale.lck"} {
		if _, err := os.Stat(filepath.Join(dst, kept)); err != nil {
			t.Fatalf("identity file %s must survive the wipe: %v", kept, err)
		}
	}
	for _, gone := range []string{"cluster/recording.log", "archive/archive.catalog"} {
		if _, err := os.Stat(filepath.Join(dst, gone)); !os.IsNotExist(err) {
			t.Fatalf("state file %s must be wiped", gone)
		}
	}

	copied, err := copyStateDirs(src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if copied != 4 { // recording.log, archive.catalog, 0-1.rec, sub/seg.log
		t.Fatalf("expected 4 files copied, got %d", copied)
	}
	// Target keeps ITS identity (content check: file body is its own name,
	// so a transplanted file would still pass Stat but fail this).
	b, _ := os.ReadFile(filepath.Join(dst, "cluster/cluster-mark.dat"))
	if string(b) != "cluster-mark.dat" {
		t.Fatal("target's cluster-mark.dat was overwritten by the source's")
	}
	if _, err := os.Stat(filepath.Join(dst, "archive/sub/seg.log")); err != nil {
		t.Fatalf("nested state must be copied: %v", err)
	}
	// Source identity files must not appear extra in target beyond its own
	// (cluster-mark-service-0.dat existed only on the source).
	if _, err := os.Stat(filepath.Join(dst, "cluster/cluster-mark-service-0.dat")); !os.IsNotExist(err) {
		t.Fatal("source identity file cluster-mark-service-0.dat must not be transplanted")
	}
}

func TestReseedNodeValidation(t *testing.T) {
	// Reseed validates node ids against the cluster descriptor, so give it one.
	c := &Cluster{}
	c.SetNodeCount(3)
	ops := &OperationsService{progress: NewProgress(), cluster: c}
	if err := ops.ReseedNode(0, 0, true); err == nil {
		t.Fatal("same source and target must be rejected")
	}
	if err := ops.ReseedNode(0, 3, true); err == nil {
		t.Fatal("out-of-range source must be rejected")
	}
	if err := ops.ReseedNode(1, 2, false); err == nil {
		t.Fatal("force=false must be rejected (quorum outage confirmation)")
	}
}
