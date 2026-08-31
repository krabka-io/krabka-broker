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

[Unreleased]: https://github.com/krabka-io/krabka-broker/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.1
[0.5.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.0
