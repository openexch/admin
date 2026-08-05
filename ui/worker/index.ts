// Admin console edge Worker.
//
// One same-origin surface at admin.openexchange.dev: this Worker serves the built
// admin UI (the ASSETS binding = ui/dist) and reverse-proxies the two API
// backends the console talks to. Because the UI and the APIs share this one
// origin, there is NO cross-origin request and thus NO CORS at all (which is
// why the gateway can drop its wildcard Access-Control-Allow-Origin).
//
// Auth is Cloudflare Access in front of admin.openexchange.dev (human identity) plus
// the gateway's own ADMIN_AUTH_TOKEN, which the Worker presents to the admin-api
// origin as X-Admin-Token (so the origin is not open to anything but this Worker
// — anything without the token gets 401). The OMS risk endpoints additionally
// need an OMS admin bearer, injected here so the browser never holds it.
//
// Everything that is not an API path falls through to the static admin UI
// (SPA fallback in wrangler serves index.html for client-side routes).

export interface Env {
  ASSETS: Fetcher;
  // Tunneled origins (set as vars in wrangler.jsonc):
  ADMIN_GATEWAY_ORIGIN: string; // e.g. https://admin-api.openexchange.dev  -> localhost:8082
  OMS_ORIGIN: string;           // e.g. https://oms.openexchange.dev
  // Secrets (wrangler secret put):
  ADMIN_API_TOKEN?: string;         // the gateway's ADMIN_AUTH_TOKEN, sent as X-Admin-Token
  OMS_ADMIN_TOKEN?: string;         // bearer for the OMS /api/v1/admin/risk endpoints
}

const ADMIN_API_PREFIX = "/api/admin/";
const OMS_RISK_PREFIX = "/api/v1/admin/risk";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname.startsWith(ADMIN_API_PREFIX)) {
      // The gateway requires its bearer (ADMIN_AUTH_TOKEN); present it as X-Admin-Token.
      return proxy(request, env.ADMIN_GATEWAY_ORIGIN, url, { adminToken: env.ADMIN_API_TOKEN });
    }
    if (url.pathname.startsWith(OMS_RISK_PREFIX)) {
      // The OMS additionally needs an admin bearer; the console has no login.
      return proxy(request, env.OMS_ORIGIN, url, { bearer: env.OMS_ADMIN_TOKEN });
    }
    // Static admin UI (SPA fallback handled by the assets binding).
    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;

// proxy forwards the request to a tunneled origin, injecting the origin's auth
// (the gateway's X-Admin-Token, or a Bearer for the OMS) so the browser never
// holds it. The origins are only reachable via the tunnel and reject anything
// without the token, so a public origin hostname is not an open API.
async function proxy(
  request: Request,
  origin: string,
  url: URL,
  auth: { adminToken?: string; bearer?: string } = {},
): Promise<Response> {
  if (!origin) {
    return new Response("admin worker: origin not configured", { status: 502 });
  }
  const target = origin.replace(/\/$/, "") + url.pathname + url.search;
  const headers = new Headers(request.headers);
  if (auth.adminToken) {
    headers.set("X-Admin-Token", auth.adminToken);
  }
  if (auth.bearer) {
    headers.set("Authorization", "Bearer " + auth.bearer);
  }
  // Host must match the origin, not admin.openexchange.dev.
  headers.set("Host", new URL(origin).host);

  const init: RequestInit = {
    method: request.method,
    headers,
    redirect: "manual",
  };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
  }
  return fetch(target, init);
}
