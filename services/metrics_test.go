// SPDX-License-Identifier: Apache-2.0
package services

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

// The middleware must count by chi route PATTERN (fixed cardinality), not the
// raw URL, and must not count /metrics or /health scrapes.
func TestMetricsMiddlewareCountsByRoutePattern(t *testing.T) {
	m := &MetricsService{requests: make(map[string]int64)}

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/api/admin/processes/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for _, path := range []string{"/api/admin/processes/node0", "/api/admin/processes/node1", "/health"} {
		req := httptest.NewRequest("GET", path, nil)
		r.ServeHTTP(httptest.NewRecorder(), req)
	}

	if got := m.requests["GET|/api/admin/processes/{name}|200"]; got != 2 {
		t.Fatalf("expected 2 counts on the route pattern, got %d (map: %v)", got, m.requests)
	}
	for k := range m.requests {
		if k == "GET|/health|200" {
			t.Fatalf("/health must not be counted, map: %v", m.requests)
		}
	}
}

func TestMetricsCoercions(t *testing.T) {
	if toF(int64(7), 0) != 7 || toF(3, 0) != 3 || toF("x", -1) != -1 {
		t.Fatal("toF coercion wrong")
	}
	if boolF(true) != 1 || boolF(nil) != 0 {
		t.Fatal("boolF coercion wrong")
	}
	if boolS("HEALTHY", HealthHealthy) != 1 || boolS(nil, HealthHealthy) != 0 {
		t.Fatal("boolS coercion wrong")
	}
}
