// SPDX-License-Identifier: Apache-2.0
package services

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openexch/admin/agent"
)

// ProcessManager is the in-process "LocalAgent": the single-box
// implementation of the gateway↔agent contract.
var _ agent.ProcessAgent = (*ProcessManager)(nil)

// Close implements agent.ProcessAgent: stop monitors and auto-restarts
// without touching the managed processes (KillMode=process semantics).
func (pm *ProcessManager) Close() { pm.Shutdown() }

// TailLog returns the last n lines (capped at 500) of a service's log file.
// Accepts the same service-name spellings as the HTTP log API.
func (pm *ProcessManager) TailLog(service string, lines int) ([]string, error) {
	name := service
	switch service {
	case "market-gateway":
		name = "market"
	case "order-management":
		name = "oms"
	case "admin-gateway":
		name = "admin"
	}
	if strings.ContainsAny(name, "/\\") {
		return nil, fmt.Errorf("invalid service name %q", service)
	}
	f, err := os.Open(filepath.Join(pm.logDir, name+".log"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if lines > 500 {
		lines = 500
	}
	if lines < 0 {
		// A negative count (e.g. an unvalidated wire int32) would make
		// start = len(all) - lines exceed len(all) and panic the slice below.
		lines = 0
	}
	var all []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		all = append(all, scanner.Text())
	}
	if start := len(all) - lines; start > 0 {
		all = all[start:]
	}
	return all, nil
}

// NodeCounters reads a cluster node's Aeron counters from the local CnC file.
func (pm *ProcessManager) NodeCounters(nodeID int) (*agent.CounterData, error) {
	pm.countersOnce.Do(func() { pm.counters = NewAeronCounters() })
	return pm.counters.GetNodeCounters(nodeID)
}

// InstallArtifact streams content to a temp file next to DestPath, verifies
// the sha256, then atomically renames. A partial write is never visible at
// DestPath.
func (pm *ProcessManager) InstallArtifact(spec agent.ArtifactSpec, content io.Reader) error {
	if spec.DestPath == "" {
		return fmt.Errorf("artifact dest path is empty")
	}
	dir := filepath.Dir(spec.DestPath)
	tmp, err := os.CreateTemp(dir, ".artifact-*")
	if err != nil {
		return fmt.Errorf("create temp in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op after successful rename

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), content); err != nil {
		tmp.Close()
		return fmt.Errorf("write artifact: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if spec.Sha256 != "" {
		got := hex.EncodeToString(h.Sum(nil))
		if !strings.EqualFold(got, spec.Sha256) {
			return fmt.Errorf("artifact sha256 mismatch: got %s want %s", got, spec.Sha256)
		}
	}
	mode := spec.Mode
	if mode == 0 {
		mode = 0644
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, spec.DestPath); err != nil {
		return fmt.Errorf("activate artifact: %w", err)
	}
	return nil
}

// --- event stream ---

type eventHub struct {
	mu   sync.Mutex
	subs map[int]chan agent.Event
	next int
}

// Subscribe implements agent.ProcessAgent. Sends are non-blocking: a slow
// subscriber loses events rather than wedging the crash path.
func (pm *ProcessManager) Subscribe(buf int) (<-chan agent.Event, func()) {
	if buf <= 0 {
		buf = 64
	}
	pm.events.mu.Lock()
	defer pm.events.mu.Unlock()
	if pm.events.subs == nil {
		pm.events.subs = map[int]chan agent.Event{}
	}
	id := pm.events.next
	pm.events.next++
	ch := make(chan agent.Event, buf)
	pm.events.subs[id] = ch
	return ch, func() {
		pm.events.mu.Lock()
		defer pm.events.mu.Unlock()
		if _, ok := pm.events.subs[id]; ok {
			delete(pm.events.subs, id)
			close(ch)
		}
	}
}

// emitEvent fans out to subscribers without ever blocking.
func (pm *ProcessManager) emitEvent(t agent.EventType, service string, pid int, detail string) {
	ev := agent.Event{Type: t, Service: service, PID: pid, Detail: detail, At: time.Now()}
	pm.events.mu.Lock()
	defer pm.events.mu.Unlock()
	for _, ch := range pm.events.subs {
		select {
		case ch <- ev:
		default: // full: drop for this subscriber
		}
	}
}
