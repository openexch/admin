// SPDX-License-Identifier: Apache-2.0
// Admin-console config knobs. The admin-gateway base itself lives in
// components/admin/api.ts as ADMIN_BASE (VITE_ADMIN_API_URL); this file carries
// only the few extra values the admin surface reads.

/** Grafana for the admin console's header link. The default assumes the
 *  operator sits on the deployment box (Grafana binds loopback :3000);
 *  override at build time if it is ever exposed on a real hostname. */
export const GRAFANA_URL: string = import.meta.env.VITE_GRAFANA_URL || 'http://localhost:3000';

/** Where the header "Trading" back-link points. Empty/'/' keeps it same-origin;
 *  set it to the trading UI's origin when the admin console is served from its
 *  own host. */
export const TRADING_URL: string = import.meta.env.VITE_TRADING_URL || '/';

/** OMS REST base ('' = same-origin). The Risk tab (useRiskConfig) is the only
 *  admin surface that talks to the OMS rather than the admin gateway; its
 *  endpoints live under /api/v1/admin/risk on the order gateway. */
export const API_BASE: string = import.meta.env.VITE_ORDER_API_URL || '';

/** Authorization header for the OMS risk-config calls. The standalone admin
 *  console has no login of its own, so this returns no bearer by default;
 *  auth for those calls is a deployment concern (edge auth, or a token the
 *  admin worker injects when it proxies /api/v1/admin/risk). */
export function getAuthHeaders(): Record<string, string> {
  return {};
}
