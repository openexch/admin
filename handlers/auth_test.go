// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func authedServer(token string) http.Handler {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return AuthMiddleware(token)(ok)
}

func status(h http.Handler, path string, header, value string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if header != "" {
		req.Header.Set(header, value)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestNoTokenConfiguredPassesThrough(t *testing.T) {
	h := authedServer("")
	if got := status(h, "/api/admin/status", "", ""); got != http.StatusOK {
		t.Fatalf("dev mode (no token) should pass through, got %d", got)
	}
}

func TestTokenRequiredWhenConfigured(t *testing.T) {
	h := authedServer("s3cret")

	if got := status(h, "/api/admin/status", "", ""); got != http.StatusUnauthorized {
		t.Fatalf("missing token should 401, got %d", got)
	}
	if got := status(h, "/api/admin/status", "Authorization", "Bearer wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token should 401, got %d", got)
	}
	if got := status(h, "/api/admin/status", "Authorization", "Bearer s3cret"); got != http.StatusOK {
		t.Fatalf("bearer token should pass, got %d", got)
	}
	if got := status(h, "/api/admin/status", "X-Admin-Token", "s3cret"); got != http.StatusOK {
		t.Fatalf("X-Admin-Token should pass, got %d", got)
	}
}

func TestHealthAlwaysExempt(t *testing.T) {
	h := authedServer("s3cret")
	if got := status(h, "/health", "", ""); got != http.StatusOK {
		t.Fatalf("/health must not require credentials, got %d", got)
	}
}
