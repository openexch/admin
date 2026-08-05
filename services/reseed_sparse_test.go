// SPDX-License-Identifier: Apache-2.0
package services

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func allocatedOf(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Blocks * 512
}

// Aeron pre-allocates archive segments, so they are mostly holes. A copy that
// materialises those holes inflates the destination to the apparent size: on
// 2026-07-25 a ~193MB reseed wrote 4.4GB into /dev/shm, filled tmpfs, and cost
// the matching engine its quorum for ~20 minutes.
func TestCopyFileSparsePreservesHoles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "segment.rec")
	dst := filepath.Join(dir, "copy.rec")

	// 16MB apparent, with a small run of real data at the front — the shape of a
	// freshly rolled Aeron segment.
	const apparent = 16 << 20
	payload := bytes.Repeat([]byte("aeron"), 4096)

	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(apparent); err != nil {
		t.Fatal(err)
	}
	f.Close()

	srcAlloc := allocatedOf(t, src)
	if srcAlloc >= apparent {
		t.Skipf("filesystem does not support sparse files (src allocated %d)", srcAlloc)
	}

	if err := copyFileSparse(src, dst); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Logical size must survive, holes and all.
	di, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if di.Size() != apparent {
		t.Fatalf("apparent size = %d, want %d", di.Size(), apparent)
	}

	// The whole point: the copy must not materialise the hole.
	dstAlloc := allocatedOf(t, dst)
	if dstAlloc >= apparent/2 {
		t.Fatalf("hole was materialised: destination allocated %d bytes of a %d byte file", dstAlloc, apparent)
	}

	// And the bytes must still be right.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != apparent || !bytes.Equal(got[:len(payload)], payload) {
		t.Fatal("copied content does not match the source")
	}
	for _, c := range got[len(payload):] {
		if c != 0 {
			t.Fatal("hole region is not zero-filled on read")
		}
	}
}

// A file with no holes must copy byte-for-byte.
func TestCopyFileSparseHandlesDenseFiles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "dense.bin")
	dst := filepath.Join(dir, "dense-copy.bin")

	data := bytes.Repeat([]byte{0xAB}, sparseCopyBlock*2+7) // spans blocks unevenly
	if err := os.WriteFile(src, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := copyFileSparse(src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("dense copy corrupted the content")
	}
}

// A file that is entirely a hole still has to come back the right size.
func TestCopyFileSparseHandlesAllHole(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "empty.rec")
	dst := filepath.Join(dir, "empty-copy.rec")

	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	const size = 4 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if err := copyFileSparse(src, dst); err != nil {
		t.Fatal(err)
	}
	di, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if di.Size() != size {
		t.Fatalf("size = %d, want %d", di.Size(), size)
	}
}

// The space check must size by ALLOCATED blocks, not apparent size, or a sparse
// archive looks far too big to ever reseed.
func TestReseedSpaceCheckSizesByAllocation(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "node1")
	if err := os.MkdirAll(filepath.Join(src, "archive"), 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(src, "archive", "0-0.rec"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(64 << 20); err != nil { // 64MB apparent, ~0 allocated
		t.Fatal(err)
	}
	f.Close()

	got, err := allocatedBytes(filepath.Join(src, "archive"))
	if err != nil {
		t.Fatal(err)
	}
	if got > 1<<20 {
		t.Fatalf("allocated = %d, want ~0 for a sparse file (apparent size leaked in)", got)
	}

	dst := filepath.Join(dir, "node2")
	if err := os.MkdirAll(dst, 0755); err != nil {
		t.Fatal(err)
	}
	if err := checkReseedSpace(src, dst); err != nil {
		t.Fatalf("refused a reseed that easily fits: %v", err)
	}
}
