// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/openexch/admin/config"
)

// An unpinned profile must boot AE on small hosts that do not have cores 20-23.
// Check every member, including a topology beyond the usual three replicas.
func TestAssetsNodesHonorProfilePinning(t *testing.T) {
	profiles, err := config.LoadProfiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, pinning := range []string{"none", "dedicated"} {
		for _, count := range []int{1, 3, 5} {
			t.Run(fmt.Sprintf("%s/%d", pinning, count), func(t *testing.T) {
				profile := profiles["demo"]
				profile.Pinning = pinning
				cfg := &config.Config{ProjectDir: t.TempDir(), AssetsJar: "/tmp/assets.jar"}
				cfg.SetNodeCount("assets", count)
				found := 0
				for _, def := range buildServiceCatalog(cfg, profile) {
					if !strings.HasPrefix(def.Name, "ae") || def.Role != RoleClusterNode {
						continue
					}
					found++
					want := []string{"/usr/bin/java"}
					if pinning == "dedicated" {
						want = []string{"/usr/bin/taskset", "-c", "20-23", "/usr/bin/java"}
					}
					if len(def.Command) < len(want) || !slices.Equal(def.Command[:len(want)], want) {
						t.Errorf("%s: launch prefix = %v, want %v", def.Name, def.Command, want)
					}
					if pinning == "none" && slices.Contains(def.Command, "/usr/bin/taskset") {
						t.Errorf("%s: unpinned profile contains taskset", def.Name)
					}
				}
				if found != count {
					t.Fatalf("checked %d assets nodes, want %d", found, count)
				}
			})
		}
	}
}
