# Changelog

This file records the releases of krabka-broker. Every version heading below
names an annotated git tag in this repository, and the tag is what a person
quotes in an incident. [Releasing](docs/releasing.md) gives the commands that
cut one.

Krabka versions and tags the whole workspace as one unit. The version comes
from `[workspace.package]` in the root `Cargo.toml`, and each crate takes it
from there, so no crate has a release of its own. Krabka is before 1.0 and it
is undeployed, so a minor bump is free to break an interface. Read the entries
rather than the number.

The layout follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The history before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka), which still publishes
the `krabka-*` names to crates.io.

## [Unreleased]

### Added

- KIP-966 eligible leader replicas. The controller maintains the ELR, elects
  from it on failover, and `DescribeTopicPartitions` reports its columns from
  the metadata image. A broker that cannot prove it shut down cleanly drops its
  membership rather than being re-derived into it from a stale ISR.
- The Kafka controller listener routes the whole Admin surface, and serves
  KIP-590 `Envelope`. The broker sends `BrokerHeartbeat` to it.
- `BROKER_LOGGER` as a config resource, so log levels change without a restart.
- Request latency split into its local, remote and throttle phases.
- Creusot proofs for the remaining safety-critical algorithms, and the catalog
  that records which algorithm each session covers.
- `/healthz` and `/readyz` on their own listener, and reference Kubernetes
  manifests under `packaging/k8s/` that wire both probes and the format step.
  Readiness waits for log-dir recovery, bound listeners, and a metadata offset
  within `--readiness-max-metadata-lag` of the quorum's committed offset, which
  the KRaft engine and the metadata observer now track separately from the
  offset this node has applied.
- `krabka-broker --print-config-schema` prints the JSON schema of the config
  file, and [`docs/config-reference.md`](docs/config-reference.md) is
  generated from it, with example `broker.toml` files under `docs/examples/`.
- An operator guide under [`docs/operations/`](docs/operations/README.md):
  deploy, capacity and metrics, a reference Grafana dashboard, Prometheus
  alert rules, and one runbook per alert.
- A generated [KIP matrix](docs/KIP_MATRIX.md), and a design document per
  subsystem under each crate's `docs/`.
- CI gates for the KFC template, the design documents, the generated config
  reference and the metrics contract.
- A README for every crate, `SECURITY.md` and `CODEOWNERS`.
- The rustdoc set, published to GitHub Pages on every push to `main`.

### Changed

- The audit counters are exported as `krabka_broker_audit_events_total` and
  `krabka_broker_audit_write_failures_total`. The doubled `_total_total`
  suffix is gone.
- `ListOffsets` is fenced on the request leader epoch. `DescribeConfigs`
  returns typed config metadata, and reads an empty key list as a request for
  every config. Every broker-owned topic is marked internal.
- The KIP-219 throttle delay is applied after the response is sent, and the
  echoed delay is audited against every response schema.
- Cadence loops run on a `Timer` rather than an `AsyncSleeper`.
- The Kafka error-code table and the advertised `ApiVersions` table are derived
  from, and asserted against, the pinned Kafka image.

### Fixed

- `controller_id` advertises a reachable broker rather than the quorum leader.
- A refused produce partition answers with `base_offset = -1`, and an accepted
  one reports the partition's real log start offset.
- `max.message.bytes` is enforced, and an oversized batch refused.
- An open transaction's offsets answer `UNSTABLE_OFFSET_COMMIT`.
- Committed offsets expire for a group that went empty, idle and terminal
  transactional ids expire out of `__transaction_state`, and a connection idle
  past `connections.max.idle.ms` closes.
- A failed disk's partition reads as leaderless, the way `kafka-topics` reports
  it, and offline replicas appear in `Metadata` and `DescribeTopicPartitions`.

## [0.5.2] - 2026-08-31

### Added

- Diskless partitions. `krabka.diskless` is a create-only topic config that
  puts a partition's durability behind a quorum-replicated write-ahead log in
  front of object storage, with an index log that readers project. It is
  read-only in `DescribeConfigs`, pinned for the life of the topic, and refused
  alongside `remote.storage.enable=true` or `delivery.mode=scheduled`.
- A WAL fetch is authenticated as the broker node that sent it, through a
  configured principal-to-node-ID mapping (KIP-595).
- Compaction of the diskless WAL index, and reclamation of the objects it
  leaves stale.

### Fixed

- A diskless partition needs a distributed identity and a registry before it
  spawns.
- A diskless fetch honours the byte limits the request asks for.
- A stalled index replay recovers, the first flush waits for the index
  projection to catch up, and a divergent follower offset stays out of quorum
  accounting.

## [0.5.1] - 2026-08-30

### Added

- Broker audit logging, with a crash-safe spool replay.
- WORM verification and reporting for the S3 and GCS archive backends.
- Cluster metadata restore from a controller snapshot, with a topic-ID check.
- A Markdown link check in CI, and the verification ledger it reads.

### Changed

- The throttle kernel, the producer sequencing decisions, the sparse log index
  lookup and the leader epoch lookup moved into `krabka-verified`, behind
  Creusot contracts. Callers validate their inputs before they enter a kernel.

### Fixed

- The broker rejects an unsupported request version before dispatch and answers
  with `UNSUPPORTED_VERSION`, except for the `ApiVersions` fallback.
- A `read_committed` fetch keeps aborted-transaction metadata when replication
  trails the abort marker.
- A log append rolls back, and the rollback is durable, after a write failure or
  a sync failure.
- Archive verification aborts a failed multipart upload instead of leaving it
  orphaned.

## [0.5.0] - 2026-08-30

The first release of krabka-broker as its own repository. The broker, the log,
the KRaft layer and the crates that support them moved out of
robot-head/crabka.

### Added

- Cross-topic snapshots through log-embedded barrier markers (KFC-4).
- WORM archive mode with signed integrity manifests (KFC-5).
- Offline point-in-time restore from a KIP-405 archive (KFC-3).
- Deliver-at-time visibility, for records that become readable at their time
  (KFC-1).
- A data-bearing witness broker role, and a three-site stretch profile.
- Broker-side schema validation (KFC-7).
- A freeze registry and a break-glass state machine (KFC-9).
- `krabka format`, as a library as well as a binary, so a node can be formatted
  from this repository.
- A signed image, an SBOM attestation and a provenance attestation on every
  release tag.

### Changed

- Bazel is the build and test path. It runs the format check, the Clippy lint
  aspect, coverage, the rustdoc build, the Creusot proofs, the container suites
  and the image push.
- The `crabka` namespace became `krabka`, in crate names and in identifiers.
- Files over 500 lines in the broker became cohesive modules.

### Fixed

- `ListOffsets` honours `isolation_level`, and bounds every answer rather than
  only the `LATEST` one.
- An audit stamp carries the value that its freeze signature covers.
- The release publishes the image digest that cosign signed.

[Unreleased]: https://github.com/krabka-io/krabka-broker/compare/v0.5.2...HEAD
[0.5.2]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.2
[0.5.1]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.1
[0.5.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.0
