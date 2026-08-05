// SPDX-License-Identifier: Apache-2.0
package agent

import "io"

// ProcessAgent is the gateway↔agent contract: everything the control plane
// may ask of the component owning process lifecycle on one host.
//
// Deliberately NOT here (host-local implementation details behind
// Start/Stop): dependency gating, restart cascades, rapid-crash disarm,
// PID adoption, port-orphan cleanup, log rotation. Those need sub-second
// local reactions and must survive control-plane outage; the control plane
// observes them through Subscribe, never mediates them.
type ProcessAgent interface {
	// --- process verbs (1:1 with the admin HTTP surface) ---
	List() []ProcessInfo
	Get(name string) *ProcessInfo
	Start(name string) error          // with dependency check
	StartUnchecked(name string) error // orchestration path: deps already sequenced by the caller
	Stop(name string) error           // with reverse-dependency check
	ForceStop(name string) error
	Restart(name string) error
	StartAll() []ActionResult
	StopAll() []ActionResult

	// --- host-local observability the control plane needs across the wire ---
	// TailLog returns the last n lines of a service's log file.
	TailLog(service string, lines int) ([]string, error)
	// NodeCounters reads a cluster node's Aeron counters from its local CnC
	// file. Cross-node comparisons (rolling-update catch-up) happen
	// control-plane-side from per-agent reports.
	NodeCounters(nodeID int) (*CounterData, error)

	// InstallArtifact streams content into spec.DestPath: temp file on the
	// same filesystem, sha256 verify, atomic rename. Partial writes are
	// never visible at DestPath.
	InstallArtifact(spec ArtifactSpec, content io.Reader) error

	// Subscribe returns a lifecycle event channel and its unsubscribe
	// function. Buffered by buf; sends are non-blocking (drop on full).
	Subscribe(buf int) (<-chan Event, func())

	// Close stops monitors and auto-restarts WITHOUT touching the managed
	// processes (they outlive the agent and are re-adopted on the next run).
	Close()
}
