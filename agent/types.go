// SPDX-License-Identifier: Apache-2.0

// Package agent defines the contract between the admin gateway's control
// plane and a process agent — the component that owns local process
// lifecycle on one host. Today the only implementation is the in-process
// ProcessManager (the "LocalAgent"); the multi-host agentd daemon implements
// the same contract over gRPC later. See docs/AGENT-ARCHITECTURE.md.
package agent

import (
	"os"
	"time"
)

// ServiceRole classifies a managed service for the UI/status grouping.
type ServiceRole string

const (
	RoleClusterNode ServiceRole = "cluster"
	RoleGateway     ServiceRole = "gateway"
	RoleInfra       ServiceRole = "infra"
)

// ProcessInfo is the live state of a managed service. JSON shape is part of
// the admin API surface (UI + Makefile consumers) — do not change tags.
type ProcessInfo struct {
	Name         string      `json:"name"`
	Display      string      `json:"display"`
	Role         ServiceRole `json:"role"`
	Port         int         `json:"port,omitempty"`
	Running      bool        `json:"running"`
	PID          int         `json:"pid,omitempty"`
	MemoryBytes  int64       `json:"memoryBytes,omitempty"`
	CPUPercent   float64     `json:"cpuPercent,omitempty"`
	UptimeMs     int64       `json:"uptimeMs,omitempty"`
	StartedAt    string      `json:"startedAt,omitempty"`
	RestartCount int         `json:"restartCount"`
	Enabled      bool        `json:"enabled"`
	Status       string      `json:"status"` // "running", "stopped", "starting", "stopping", "failed", "crashed"
	// Why the service is failed/crashed (start error, crash exit + log tail,
	// or crash-loop disarm). Empty while healthy.
	LastError string `json:"lastError,omitempty"`
}

// ActionResult is the outcome of one service action in a bulk operation.
type ActionResult struct {
	Service string `json:"service"`
	Action  string `json:"action,omitempty"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// CounterData holds Aeron cluster counters for a node, read host-locally
// from the media driver's CnC file.
type CounterData struct {
	CommitPosition int64 // Cluster commit position (real-time)
	SnapshotCount  int64 // Number of snapshots taken
	NodeRole       int64 // 0=follower, 1=candidate, 2=leader
}

// ArtifactSpec describes an artifact installation: stream content to a temp
// file on the destination filesystem, verify the sha256, then atomically
// rename over DestPath. Shaped so the future remote version (chunked
// Stage + Activate over gRPC) is a refinement, not a break.
type ArtifactSpec struct {
	DestPath string
	Sha256   string // hex; empty = skip verification (discouraged)
	Mode     os.FileMode
}

// EventType classifies agent lifecycle events.
type EventType string

const (
	EventStarted     EventType = "started"
	EventStopped     EventType = "stopped"
	EventCrashed     EventType = "crashed"
	EventCascadeStop EventType = "cascade-stop"
	EventDisarmed    EventType = "disarmed" // crash-loop cap tripped; auto-restart off
	EventAdopted     EventType = "adopted"  // re-attached to a live PID after agent restart
)

// Event is one agent lifecycle notification. Delivery is best-effort with
// bounded buffers: a slow subscriber loses events rather than wedging the
// crash path.
type Event struct {
	Type    EventType `json:"type"`
	Service string    `json:"service"`
	PID     int       `json:"pid,omitempty"`
	Detail  string    `json:"detail,omitempty"`
	At      time.Time `json:"at"`
}
