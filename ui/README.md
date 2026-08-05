# Admin UI

The Open Exchange cluster admin console as a standalone React + TypeScript +
Vite single-page app. It was extracted from the trading UI (where it lived at
the `/admin` route) so it can build and deploy on its own, next to the Go admin
gateway in this repo.

Tabs: Overview, Clusters, Services, Profiles, Risk, Backup.

## Configuration

Build-time env vars (Vite `import.meta.env`):

- `VITE_ADMIN_API_URL` — admin-gateway origin for `/api/admin/*` calls and the
  SSE event stream. **Empty = same-origin**, which is the target deploy (the
  console is served from the same host that proxies the admin gateway). Set it
  to e.g. `http://localhost:8082` for local dev against a running gateway.
- `VITE_GRAFANA_URL` — the header "Grafana" link (default
  `http://localhost:3000`).
- `VITE_TRADING_URL` — where the header "Trading" back-link points (default
  `/`). Set to the trading UI origin when admin is on its own host.
- `VITE_ORDER_API_URL` — OMS origin for the Risk tab only (empty = same-origin).
  The Risk tab reads/writes risk config on the order gateway under
  `/api/v1/admin/risk`; auth for those calls is a deployment concern (the
  standalone console has no login of its own).

## Develop / build / test

```sh
npm install
npm run dev      # vite dev server
npm run build    # tsc && vite build -> dist/
npm test         # vitest run
```

`npm run build` emits a static bundle to `dist/`. That `dist/` is what gets
served as the admin console; wiring it up as a Cloudflare Worker (and proxying
`/api/admin` to the gateway) is a separate deployment step, not part of this
subproject.

## Deployment (admin.openexchange.dev, behind Cloudflare Access)

This UI deploys as a single Cloudflare Worker (`worker/index.ts`, `wrangler.jsonc`)
that serves the built assets AND reverse-proxies the two API backends at the SAME
origin, so there is no CORS.

```
npm run build     # vite -> ./dist
npm run deploy    # wrangler deploy (uploads dist + the Worker)
```

Routes: `/api/admin/*` -> the admin gateway origin; `/api/v1/admin/risk*` -> the OMS
(with an injected admin bearer, since the console has no login); everything else ->
the static admin UI.

### One-time Cloudflare setup (dashboard + cloudflared, not in the repo)

1. **Tunnel origins** (cloudflared ingress): `admin-api.openexchange.dev -> http://localhost:8082`
   (the gateway). `oms.openexchange.dev` is already tunneled.
2. **Cloudflare Access** (the permanent fix for the admin-exposure P0):
   - App on **`admin.openexchange.dev`**: email/SSO policy = the humans who may operate the stack.
   - App on **`admin-api.openexchange.dev`**: **service-token** policy, so ONLY this Worker
     (presenting the token) can reach the gateway; nothing else can.
3. **Secrets** (`wrangler secret put ...`): `CF_ACCESS_CLIENT_ID`, `CF_ACCESS_CLIENT_SECRET`
   (the service token from step 2), `OMS_ADMIN_TOKEN` (bearer for the OMS risk endpoints).
4. **Vars** (`wrangler.jsonc`): confirm `ADMIN_GATEWAY_ORIGIN` / `OMS_ORIGIN` match the tunnel
   hostnames. Enable the `routes` entry once `admin.openexchange.dev` is on the zone behind Access.

Rollback: remove the `routes` entry and redeploy (or keep admin off the tunnel entirely, as it
is now post-incident, and reach it via `wrangler dev` / loopback).
