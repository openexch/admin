# Contributing to admin

Thanks for your interest in Open Exchange. `admin` is the Go process
manager and operations API for the stack: it supervises the cluster nodes,
gateways, media drivers, and OMS, and exposes `/api/admin/*` for operations
(status, snapshots, backups, rolling updates, reseed).

## Before you start

- For anything larger than a small fix, open an issue first.
- Operational changes should come with the matching runbook update in
  `docs/RUNBOOKS.md`.

## Development setup

- Go (version pinned in `go.mod`).

```bash
go build ./...
go vet ./...
go test ./...
```

CI runs the same commands plus `govulncheck` and Trivy vulnerability and
secret scanning.

## Design constraints

- **Never lie about cluster health.** `/api/admin/status` reports process
  liveness and counter freshness truthfully; guards (lag guard, member-down
  refusal, operation claims) exist so that destructive operations cannot run
  against an unhealthy cluster. Do not weaken them for convenience.
- **Archive safety.** Anything touching Aeron archives or live driver
  directories must preserve the existing do-not-delete-live-dir guards.
- **Secure by default.** The API binds to loopback unless a bearer token is
  configured; keep that invariant.

## Pull requests

- **One logical change per PR.** Each PR is squash-merged into exactly one
  commit on `main` (linear history).
- **Sign your commits.** `main` requires signed commits; unsigned PR heads
  cannot be merged.
- Commit/PR title style: `type: imperative summary` with types
  `feat|fix|docs|test|ci|chore`.

## License

Apache-2.0. By contributing, you agree that your contributions are licensed
under the same terms as the project (inbound = outbound). No CLA.
