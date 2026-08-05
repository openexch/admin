// SPDX-License-Identifier: Apache-2.0
package services

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/agent/agenttest"
)

// TestLocalAgentConformance runs the executable ProcessAgent specification
// against the in-process LocalAgent. The remote agentd client must pass the
// exact same suite (Horizon B loopback parity).
func TestLocalAgentConformance(t *testing.T) {
	oldStagger := startStagger
	startStagger = 50 * time.Millisecond
	t.Cleanup(func() { startStagger = oldStagger })

	agenttest.RunConformance(t, func(t *testing.T) (agent.ProcessAgent, agenttest.Fixture) {
		dir := t.TempDir()
		logDir := filepath.Join(dir, "logs")
		scratch := filepath.Join(dir, "scratch")
		if err := os.MkdirAll(scratch, 0755); err != nil {
			t.Fatal(err)
		}
		pm := NewProcessManagerWith(ProcessManagerOptions{
			Services: []ServiceDef{
				{Name: "conf-a", Display: "Conformance A", Role: RoleInfra,
					Command: []string{"/bin/sh", "-c", "sleep 60"}, StopTimeout: 1},
				{Name: "conf-b", Display: "Conformance B", Role: RoleInfra,
					Command:     []string{"/bin/sh", "-c", "sleep 60"},
					AutoRestart: true, RestartSec: 1, StopTimeout: 1},
			},
			LogDir: logDir,
			PidDir: filepath.Join(dir, "pids"),
		})
		t.Cleanup(func() {
			pm.StopAll()
			pm.Close()
		})
		return pm, agenttest.Fixture{
			Simple:      "conf-a",
			AutoRestart: "conf-b",
			LogDir:      logDir,
			Scratch:     scratch,
		}
	})
}
