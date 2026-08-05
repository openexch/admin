// SPDX-License-Identifier: Apache-2.0
package services

// Node health derivation for /api/admin/status truthfulness (issue #13 item 1).
//
// The CnC counter files under /dev/shm survive process death, so counter values
// alone can describe a node that no longer exists. Health is therefore derived
// from process liveness plus counter freshness, where freshness means the commit
// position changed across the status poller's cycles. A frozen commit position
// only counts against a node while the rest of the cluster is advancing:
// at zero load every position is legitimately frozen.

// FrozenGracePolls is how many consecutive polls a node's commit position may
// stay frozen (while other nodes advance) before it is reported DEGRADED.
// At the 2s poll interval this is a 10s grace window, enough to ride out an
// election without flapping.
const FrozenGracePolls = 5

// Node health values reported in the status JSON.
const (
	HealthHealthy  = "HEALTHY"
	HealthDegraded = "DEGRADED"
	HealthDead     = "DEAD"
	HealthOffline  = "OFFLINE"
)

// NodeObservation is one poll's worth of evidence about a cluster node.
type NodeObservation struct {
	PmRunning    bool // ProcessManager thinks the process is running
	PidAlive     bool // kill(pid, 0) succeeded this poll
	CncOK        bool // CnC file readable and commit-pos counter present
	FrozenPolls  int  // consecutive polls frozen while others advanced (see UpdateFrozenPolls)
	Transitional bool // tracked STARTING/STOPPING/REJOINING/ELECTION state
}

// DeriveNodeHealth maps one poll's observation to a health value and the
// truthful "running" flag. A PM-running process whose PID is gone is DEAD and
// reported running=false: that is the exact lie this exists to correct.
func DeriveNodeHealth(o NodeObservation) (health string, running bool) {
	switch {
	case !o.PmRunning:
		return HealthOffline, false
	case !o.PidAlive:
		return HealthDead, false
	case !o.CncOK:
		return HealthDegraded, true
	case !o.Transitional && o.FrozenPolls >= FrozenGracePolls:
		return HealthDegraded, true
	default:
		return HealthHealthy, true
	}
}

// UpdateFrozenPolls advances the consecutive-frozen counter for one node.
// The counter only accumulates while this node's commit position is stuck AND
// some other node advanced this poll; anything else (own progress, cluster-wide
// quiet, transitional state) resets it.
func UpdateFrozenPolls(prev int, selfAdvanced, othersAdvanced, transitional bool) int {
	if transitional || selfAdvanced || !othersAdvanced {
		return 0
	}
	return prev + 1
}

// cncRoleString renders the "Cluster node role" counter value.
func cncRoleString(v int64) string {
	switch v {
	case 0:
		return "FOLLOWER"
	case 1:
		return "CANDIDATE"
	case 2:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}
