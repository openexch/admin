// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// AuthMiddleware enforces the admin API bearer token (admin-gateway#11).
//
// With a token configured, every route except /health and /metrics requires
// "Authorization: Bearer <token>" (or "X-Admin-Token: <token>"); mismatches
// get 401. With no token configured it passes everything through — main only
// permits that combination on a loopback bind. /metrics is exempt for the
// local Prometheus scraper, matching the OMS convention (read-only, no
// secrets; deployments exposing the admin port must firewall it anyway).
func AuthMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" || r.URL.Path == "/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			if subtle.ConstantTimeCompare([]byte(requestToken(r)), []byte(token)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
		})
	}
}

func requestToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return r.Header.Get("X-Admin-Token")
}
