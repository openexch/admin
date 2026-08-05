# Changelog

All notable changes to `admin` (the Open Exchange process manager and
operations API) are documented here. The stack (`match`, `assets`, `oms`,
`admin`, `trading-ui`) is versioned together; one version spans all five repos.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

First release of the admin core as its own repository: everything one operator
needs to bring up one cluster on one box, keep it alive, and get it back after
a failure.

- Process supervision with dependency-ordered start/stop, crash-loop capping,
  auto-restart and persisted desired state.
- Cluster node lifecycle, per node and for the whole set.
- Status and health derived from Aeron's CnC counters, so a node that died
  without releasing them is reported dead rather than healthy.
- Snapshots: manual, and automatic on a byte trigger with a time floor.
- Retention: live archive housekeeping and settlement-journal purging, both
  bounded by a watermark computed from what every consumer has actually passed.
- Recovery: cleanup, backup restore, and reseed of a stranded member from a
  healthy follower.
- Pre-flight invariants that refuse a destructive operation on a box that
  cannot survive it.
- Prometheus metrics, structured JSON logs with request and operation
  correlation ids, and an SSE event stream.
- A React console over all of the above.

### Notes

Operations that coordinate across boxes, across time or across a build pipeline
are deliberately not here. This repository is the half that runs and survives a
cluster; releases, rolling updates, fleet management and off-box archival live
above it.
