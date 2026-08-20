# krabka-broker

The [krabka](https://github.com/krabka-io) broker: an Apache Kafka-compatible
broker with a KRaft consensus engine, tiered storage, and its supporting
subsystems.

It builds on two sibling repositories and nothing else in the stack —
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for the wire
layer and [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs)
for the Kafka client the broker and its tests use.

## Crates

| Crate | What it is |
| --- | --- |
| `crabka-broker` | The broker: request handlers, coordinators, replication, quotas, the server binary. |
| `crabka-raft` | KRaft consensus engine. |
| `crabka-kraft-core` | Shared KRaft types and the quorum state machine. |
| `crabka-log` | The on-disk log: segments, indexes, compaction, retention. |
| `crabka-records-legacy` | v0/v1 message-set codecs for down-conversion. |
| `crabka-remote-storage` | Tiered-storage backends and the remote log reader. |
| `crabka-remote-storage-topic` | The `__remote_log_metadata` topic-backed RLMM. |
| `crabka-object-store` | Object-store abstraction shared by the tiered paths. |
| `crabka-authz` | Authorizer traits, ACL evaluation, and the OPA client. |
| `crabka-audit` | Audit event model, OCSF serialization, and the `crabka-audit verify` CLI. |
| `crabka-format` | Formats a fresh log directory: `meta.properties.json`, bootstrap records, the singleton `VotersRecord`. |
| `crabka-throttle` | Quota token buckets (Creusot-verified). |
| `crabka-verified` | Formally verified pure kernels shared by consensus and log. |
| `crabka-telemetry` | OTLP pipeline, metrics registry, and the debug/pprof routes. |
| `crabka-logfmt` | logfmt encoder for structured logs. |

## Build

Bazel is the build and test path. Cargo stays the dependency source of truth:
[`rules_rs`](https://github.com/hermeticbuild/rules_rs) reads the same
`Cargo.toml` and `Cargo.lock` that Cargo does, so there is no second dependency
set to keep in sync.

```
bazel test //...
```

Run the broker:

```
bazel run //:broker_bin -- --help
```

`cargo` works the same way it always did:

```
cargo nextest run --workspace
```

Both run the same 3392 tests. Bazel additionally runs the 9 rustdoc examples,
which `cargo nextest` cannot; `cargo test --workspace --doc` covers those.

## Depending on the siblings

Both siblings are pinned by revision in exactly one place, the
`[patch.crates-io]` block at the bottom of the root `Cargo.toml`. Member
manifests declare those crates as ordinary `crabka-x = "0.4.0"` requirements and
the patch redirects them at the git checkouts, so a manifest still reads as a
normal Cargo manifest. To move to a newer sibling, change the revision there and
re-run `cargo generate-lockfile`.

## Mutation testing

Mutation sweeps run through
[`rules_rs_mutants`](https://github.com/robot-head/rules_rs_mutants), which lets
`cargo-mutants` enumerate mutants and lets Bazel build and test them:

```
bazel test //crates/raft:raft_mutants
```

They are tagged `manual`, so `bazel test //...` skips them and a nightly job runs
the full sweep. Two things to know about the results:

* Only `#[cfg(test)]` unit tests take part. Mutants that the `tests/*.rs` suites
  would kill are reported as survivors, so scores read lower than the monorepo's
  `cargo mutants` numbers for the same code. The nightly job is therefore
  `continue-on-error`: it exists to produce a list to read, not to gate.
* The `cargo-mutants` version is pinned by `tools/mutants/Cargo.lock`, not by
  whatever is on `PATH`.

## Storage formatting

A KRaft node must have its log directory formatted before the broker will boot —
that step seeds `meta.properties.json` and the singleton `VotersRecord`, and the
broker treats an unformatted directory as operator error.

```
bazel run //:format_bin -- --log-dir /var/lib/krabka --standalone \
    --node-id 1 --controller-listener 0.0.0.0:9093
```

The monorepo spells this `crabka format`, as a subcommand of the operator CLI.
That CLI also drives the gres layer, so it could not follow the broker out; the
command needs only the metadata and security crates, so it lives here as
`crabka-format` — a library as well as a binary, so `crabka-cli` can call it
rather than carry a second copy.

## Docker-driven suites

The JVM acceptance, tiered-storage and Kerberos suites are `#[ignore]`d, exactly
as in the monorepo: they need Docker, a `cp-kafka` image, or an MIT KDC. They
build under both Cargo and Bazel and are skipped by both unless run explicitly.

## Publishing

`crabka-log` and the other published names in this tree are released to crates.io
from [`robot-head/crabka`](https://github.com/robot-head/crabka). This repository
has no release automation; consumers pin it by git revision.
