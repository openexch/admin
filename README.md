# Open Exchange — Admin Gateway

HTTP API for running the Open Exchange Aeron Cluster on a box: process lifecycle,
snapshots and retention, log retrieval, cleanup and recovery, and cluster health.

This is the core. It is everything one operator needs to bring up one cluster on
one machine, keep it alive, and get it back after a failure. Operations that
coordinate ACROSS boxes, across time or across a build pipeline are not here.

## Features

- Process management for all cluster services (nodes, drivers, gateways, backup, OMS)
- Automatic and manual snapshot creation + live archive housekeeping
- Retention watermark: the log is purged only below what every consumer has passed
- Stranded-member reseed and disk-backup recovery
- Per-node log retrieval
- HTTP health probing of managed services (OMS :8080, market gateway :8081)
- Dependency-ordered startup and shutdown
- Structured JSON logs with request/operation correlation ids
- Prometheus `/metrics`

Operational procedures live in [docs/RUNBOOKS.md](docs/RUNBOOKS.md).

## Tech Stack

- **Go 1.25**
- **chi** — HTTP routing
- Runs as a systemd user service

## Getting Started

### Prerequisites

- Go 1.25+ (pinned in `go.mod`)
- Running Aeron Cluster (match engine)

### Build & Run

```bash
go build -o admin-gateway .
./admin-gateway
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADMIN_PORT` | `8082` | HTTP server port |
| `ADMIN_BIND` | `127.0.0.1` | Listen address (loopback by default) |
| `ADMIN_AUTH_TOKEN` | _(empty)_ | Bearer token required on every route except `/health` and `/metrics` |
| `ADMIN_AUTH_TOKEN_FILE` | _(empty)_ | File to read the token from (whitespace trimmed) |
| `ADMIN_LOG_FORMAT` | `json` | Structured log format: `json` or `text` |
| `MATCH_PROJECT_DIR` | (auto-detected) | Path to the match engine project |
| `ADMIN_MIN_MEM_MB` | `4096` | Pre-flight: block gated ops when host `MemAvailable` is below this (warn below 1.5x) |
| `ADMIN_MIN_ROOT_DISK_GB` | `5` | Pre-flight: block gated ops when `/` has less free space |
| `ADMIN_MAX_SHM_USED_PCT` | `90` | Pre-flight: block gated ops when `/dev/shm` is fuller than this |
| `ADMIN_AGENT_TOKEN` / `_FILE` | _(empty)_ | Shared agent token; required (with TLS) for non-loopback agent listeners |
| `ADMIN_AGENT_TLS_CERT` / `ADMIN_AGENT_TLS_KEY` | _(empty)_ | TLS keypair for the agent listener (TLS 1.3) |

### Authentication

The admin API drives destructive operations, so it is secure by default:
it binds loopback unless `ADMIN_BIND` says otherwise, and a non-loopback
bind **refuses to start** without a token. With a token configured, send
`Authorization: Bearer <token>` (or `X-Admin-Token`) on every call; only
`GET /health` stays open for liveness probes. With no token on a loopback
bind the API is open to local processes (dev mode; a startup warning is
logged).

## API Endpoints

### Status & Monitoring

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/admin/status` | Cluster status (includes pre-flight invariants) |
| `GET` | `/api/admin/progress` | Operation progress |
| `GET` | `/api/admin/preflight` | Run all pre-flight invariant checks (report only, never a gate) |
| `GET` | `/api/admin/events` | SSE: process lifecycle events + operation progress (bearer auth like every route; tokened browser clients use fetch-streaming, never URL tokens) |
| `GET` | `/api/admin/logs?node=0&lines=100` | Service logs |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics (auth-exempt, for the local scraper) |

### Node Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/admin/restart-node` | Restart single node |
| `POST` | `/api/admin/stop-node` | Stop single node |
| `POST` | `/api/admin/start-node` | Start single node |
| `POST` | `/api/admin/stop-all-nodes` | Stop all cluster nodes |
| `POST` | `/api/admin/start-all-nodes` | Start all cluster nodes |

### Cluster Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/admin/snapshot` | Create cluster snapshot (+ live archive housekeeping) |
| `POST` | `/api/admin/housekeeping` | Reclaim archive disk on the live cluster |
| `POST` | `/api/admin/cleanup` | IPC/mark cleanup; archives preserved unless explicitly confirmed |
| `POST` | `/api/admin/cleanup-node` | Per-node cleanup |

### Process Manager

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/admin/processes` | List all managed processes |
| `GET` | `/api/admin/processes/{name}` | Get specific process |
| `POST` | `/api/admin/processes/{name}/start` | Start process |
| `POST` | `/api/admin/processes/{name}/stop` | Stop process |
| `POST` | `/api/admin/processes/{name}/restart` | Restart process |
| `POST` | `/api/admin/processes/{name}/restart` | Restart process |
| `POST` | `/api/admin/processes/{name}/force-stop` | SIGKILL process |
| `POST` | `/api/admin/processes/start-all` | Start all (dependency order) |
| `POST` | `/api/admin/processes/stop-all` | Stop all (reverse order) |

### Auto-Snapshot

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/admin/auto-snapshot` | Get auto-snapshot status |
| `POST` | `/api/admin/auto-snapshot` | Start auto-snapshot |
| `DELETE` | `/api/admin/auto-snapshot` | Stop auto-snapshot |

### Recovery

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/admin/backup-info` | Backup information |
| `POST` | `/api/admin/recover-from-backup` | Restore from backup |
| `POST` | `/api/admin/reseed-node` | Reseed a stranded member from a healthy follower (brief quorum outage; `force` required) |

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
