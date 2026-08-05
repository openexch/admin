// SPDX-License-Identifier: Apache-2.0
package services

import (
	"time"

	"github.com/openexch/admin/config"
)

// Desired-state tracking + reconcile. The operator's intent per service
// ("running"|"stopped") is recorded on every explicit Start/Stop/StartAll/
// StopAll and persisted to desired-state.json next to the admin binary. At
// boot ReconcileDesired() brings the stack back to that intent, so a reboot
// restores what the operator actually wanted:
//
//   - a service they stopped stays stopped,
//   - a service that should be running is brought back (even after a crash),
//   - a fresh box with no recorded intent stays idle.
//
// A crash or the rapid-restart DISARM deliberately does NOT change desired
// state — that is a failure, not an intent to stop — so a crashed service is
// still desired-running and gets another chance on the next boot.

// setDesired records intent for one service and persists the whole map. The
// admin service is self-managed (systemd), never tracked here. A blank
// adminDir (tests / agentd default) keeps the in-memory map but skips the
// write.
func (pm *ProcessManager) setDesired(name, state string) {
	if name == "admin" {
		return
	}
	pm.desiredMu.Lock()
	if pm.desired == nil {
		pm.desired = map[string]string{}
	}
	if pm.desired[name] == state {
		pm.desiredMu.Unlock()
		return // no change, no write
	}
	pm.desired[name] = state
	snapshot := make(map[string]string, len(pm.desired))
	for k, v := range pm.desired {
		snapshot[k] = v
	}
	pm.desiredMu.Unlock()
	pm.persistDesired(snapshot)
}

// setDesiredAll records the same intent for every managed (non-admin,
// runnable) service in one write.
func (pm *ProcessManager) setDesiredAll(state string) {
	pm.desiredMu.Lock()
	if pm.desired == nil {
		pm.desired = map[string]string{}
	}
	for _, def := range pm.services {
		if def.Name == "admin" || len(def.Command) == 0 {
			continue
		}
		pm.desired[def.Name] = state
	}
	snapshot := make(map[string]string, len(pm.desired))
	for k, v := range pm.desired {
		snapshot[k] = v
	}
	pm.desiredMu.Unlock()
	pm.persistDesired(snapshot)
}

func (pm *ProcessManager) persistDesired(snapshot map[string]string) {
	if pm.adminDir == "" {
		return
	}
	if err := config.PersistDesiredState(pm.adminDir, snapshot, time.Now()); err != nil {
		pm.log.Warn("could not persist desired state", "err", err)
	}
}

// desiredOf returns the recorded intent for a service, or "" if none.
func (pm *ProcessManager) desiredOf(name string) string {
	pm.desiredMu.Lock()
	defer pm.desiredMu.Unlock()
	return pm.desired[name]
}

// reconcileTargets returns, in boot order, the services that SHOULD be started
// to satisfy the recorded intent: managed (non-admin, runnable) services whose
// desired state is "running". Absent (no recorded intent) and "stopped" are
// both skipped. Pure — no side effects — so the policy is unit-testable
// without launching real processes.
func (pm *ProcessManager) reconcileTargets() []ServiceDef {
	var targets []ServiceDef
	for _, def := range pm.bootOrder() {
		if def.Name == "admin" || len(def.Command) == 0 {
			continue
		}
		if pm.desiredOf(def.Name) == config.DesiredRunning {
			targets = append(targets, def)
		}
	}
	return targets
}

// ReconcileDesired brings the stack to the operator's last recorded intent.
// Called once at boot (after adoptExisting). Services already running (adopted
// from PID files) are left alone; desired-running services that are down are
// started in dependency order. Returns per-service results for logging.
func (pm *ProcessManager) ReconcileDesired() []ActionResult {
	targets := pm.reconcileTargets()
	if len(targets) == 0 {
		pm.log.Info("desired-state reconcile: nothing to start (no recorded intent, or all stopped)")
		return nil
	}
	results := make([]ActionResult, 0, len(targets))
	started := 0
	for _, def := range targets {
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
		pm.rearm(def.Name)
		err := pm.startProcess(def)
		res := ActionResult{Service: def.Name, Action: "start", Success: err == nil}
		if err != nil {
			res.Error = err.Error()
		} else {
			started++
		}
		results = append(results, res)
		time.Sleep(startStagger)
	}
	pm.log.Info("desired-state reconcile complete", "desired_running", len(targets), "started", started)
	return results
}
