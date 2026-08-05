// SPDX-License-Identifier: Apache-2.0
package services

import "testing"

func TestDeriveNodeHealth(t *testing.T) {
	cases := []struct {
		name        string
		obs         NodeObservation
		wantHealth  string
		wantRunning bool
	}{
		{
			name:        "running alive advancing",
			obs:         NodeObservation{PmRunning: true, PidAlive: true, CncOK: true, FrozenPolls: 0},
			wantHealth:  HealthHealthy,
			wantRunning: true,
		},
		{
			name:        "pm running but pid dead",
			obs:         NodeObservation{PmRunning: true, PidAlive: false, CncOK: true},
			wantHealth:  HealthDead,
			wantRunning: false,
		},
		{
			name:        "not running",
			obs:         NodeObservation{PmRunning: false},
			wantHealth:  HealthOffline,
			wantRunning: false,
		},
		{
			name:        "cnc unreadable while alive",
			obs:         NodeObservation{PmRunning: true, PidAlive: true, CncOK: false},
			wantHealth:  HealthDegraded,
			wantRunning: true,
		},
		{
			name:        "frozen past grace while others advance",
			obs:         NodeObservation{PmRunning: true, PidAlive: true, CncOK: true, FrozenPolls: FrozenGracePolls},
			wantHealth:  HealthDegraded,
			wantRunning: true,
		},
		{
			name:        "frozen under grace",
			obs:         NodeObservation{PmRunning: true, PidAlive: true, CncOK: true, FrozenPolls: FrozenGracePolls - 1},
			wantHealth:  HealthHealthy,
			wantRunning: true,
		},
		{
			name:        "transitional suppresses frozen check",
			obs:         NodeObservation{PmRunning: true, PidAlive: true, CncOK: true, FrozenPolls: FrozenGracePolls + 3, Transitional: true},
			wantHealth:  HealthHealthy,
			wantRunning: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			health, running := DeriveNodeHealth(tc.obs)
			if health != tc.wantHealth || running != tc.wantRunning {
				t.Fatalf("DeriveNodeHealth(%+v) = (%s, %v), want (%s, %v)",
					tc.obs, health, running, tc.wantHealth, tc.wantRunning)
			}
		})
	}
}

func TestUpdateFrozenPolls(t *testing.T) {
	cases := []struct {
		name                                  string
		prev                                  int
		selfAdvanced, othersAdvanced, transit bool
		want                                  int
	}{
		{"frozen while others advance accumulates", 2, false, true, false, 3},
		{"own progress resets", 4, true, true, false, 0},
		{"cluster-wide quiet (zero load) resets", 4, false, false, false, 0},
		{"transitional resets", 4, false, true, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := UpdateFrozenPolls(tc.prev, tc.selfAdvanced, tc.othersAdvanced, tc.transit)
			if got != tc.want {
				t.Fatalf("UpdateFrozenPolls(%d, %v, %v, %v) = %d, want %d",
					tc.prev, tc.selfAdvanced, tc.othersAdvanced, tc.transit, got, tc.want)
			}
		})
	}
}

// Zero-load freeze must stay healthy indefinitely: others never advance, so the
// frozen counter never accumulates and health stays HEALTHY.
func TestZeroLoadFreezeStaysHealthy(t *testing.T) {
	polls := 0
	for i := 0; i < 100; i++ {
		polls = UpdateFrozenPolls(polls, false, false, false)
	}
	health, running := DeriveNodeHealth(NodeObservation{
		PmRunning: true, PidAlive: true, CncOK: true, FrozenPolls: polls,
	})
	if health != HealthHealthy || !running {
		t.Fatalf("zero-load freeze: got (%s, %v), want (HEALTHY, true)", health, running)
	}
}
