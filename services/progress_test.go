// SPDX-License-Identifier: Apache-2.0
package services

import (
	"sync"
	"sync/atomic"
	"testing"
)

// The chaos-soak bug (#10 follow-up): the old IsRunning()-then-Start() pattern
// let two operations claim the slot concurrently. TryStart must admit exactly
// one claimant, and release only via Finish.
func TestProgressTryStartExclusive(t *testing.T) {
	p := NewProgress()
	var wins int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.TryStart("op", 3) {
				atomic.AddInt32(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", wins)
	}

	if p.TryStart("op2", 1) {
		t.Fatal("slot must stay claimed while the operation is running")
	}
	p.Finish(true, "done")
	if !p.TryStart("op2", 1) {
		t.Fatal("slot must be claimable again after Finish")
	}
}
