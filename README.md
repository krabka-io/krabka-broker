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
| `krabka-broker` | The broker: request handlers, coordinators, replication, quotas, the server binary. |
| `krabka-raft` | KRaft consensus engine. |
| `krabka-kraft-core` | Shared KRaft types and the quorum state machine. |
| `krabka-log` | The on-disk log: segments, indexes, compaction, retention. |
| `krabka-records-legacy` | v0/v1 message-set codecs for down-conversion. |
| `krabka-remote-storage` | Tiered-storage backends and the remote log reader. |
| `krabka-remote-storage-topic` | The `__remote_log_metadata` topic-backed RLMM. |
| `krabka-object-store` | Object-store abstraction shared by the tiered paths. |
| `krabka-authz` | Authorizer traits, ACL evaluation, and the OPA client. |
| `krabka-audit` | Audit event model, OCSF serialization, and the `krabka-audit verify` CLI. |
| `krabka-format` | Formats a fresh log directory: `meta.properties.json`, bootstrap records, the singleton `VotersRecord`. |
| `krabka-throttle` | Quota token buckets (Creusot-verified). |
| `krabka-verified` | Formally verified pure kernels shared by consensus and log. |
| `krabka-telemetry` | OTLP pipeline, metrics registry, and the debug/pprof routes. |
| `krabka-logfmt` | logfmt encoder for structured logs. |

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

### Everything CI does, locally

The [Aspect CLI](https://github.com/aspect-build/aspect-cli) narrows each task to
what a change actually touched, which for a repository this size is the
difference between a minute and most of an hour. Every one has a plain-Bazel
equivalent, so the CLI is a convenience rather than a requirement:

| | Aspect CLI | Plain Bazel |
| --- | --- | --- |
| Build | `aspect build //...` | `bazel build //...` |
| Test | `aspect test //...` | `bazel test //...` |
| Lint | `aspect lint` | `bazel build --config=lint //...` |
| Format | `aspect format` | `bazel run //tools/format` |
| Coverage | `aspect test --coverage` | `bazel coverage //crates/...` |
| Docs | — | `bazel build //crates/audit:audit_doc` |
| Delivery | `aspect delivery` | `bazel run //packaging:push -- --tag dev` |

Formatting and linting are Bazel targets rather than a separate `cargo fmt` /
`cargo clippy` pass, so they see exactly the files and crates the build sees. A
file in no target cannot drift unnoticed, and clippy resolves the same features
the build resolves.

Two details worth knowing:

* **rustfmt runs on a pinned nightly.** `rustfmt.toml` uses
  `format_code_in_doc_comments`, `group_imports` and `imports_granularity`, all
  still nightly-gated; stable rustfmt warns and silently skips them. The nightly
  is pinned in `MODULE.bazel`, so formatting is reproducible rather than a
  function of whichever nightly is installed.
* **`rustfmt.toml` states its edition.** `cargo fmt` passes `--edition` from
  `Cargo.toml`; rustfmt invoked directly defaults to 2015 and sorts `use` lists
  differently. Stating it makes formatting a property of the repository rather
  than of how rustfmt was launched.

## Container suites

The JVM acceptance, tiered-storage and Kerberos suites drive real Kafka
containers. They are `#[ignore]`d under Cargo, and under Bazel they are separate
targets tagged `docker`, which the default `bazel test` filters out:

```
bazel test --config=docker //crates/...
```

The images are **pinned by digest in `MODULE.bazel` and fetched by Bazel**, not
pulled by testcontainers mid-test. `//bazel/images` turns each into a
`docker load`-able tarball, and `//bazel:docker_test.sh` loads them before the
suite runs. So the bytes a suite runs against are the bytes the build pinned, the
network is out of the test itself, and the repository cache carries the images
between CI runs. The JDK that `jvm_tiered_storage`'s probe needs is downloaded by
Bazel too, rather than resolved from the machine — under Cargo that suite simply
fails on a host with no JDK.

The Docker daemon is the one thing Bazel cannot own, so it is the one thing left
undeclared: those targets are tagged `no-sandbox` for the socket and `external`
so a pass is never cached against inputs that do not describe the daemon's state.

They run nightly rather than per-PR — the images alone are a few gigabytes.

`describe_groups_jvm` is *not* among them. It records a real-Kafka fixture into
`tests/fixtures/` rather than reading one, which makes it a recording tool: a
Bazel test cannot write to the source tree, and should not want to. It stays
`#[ignore]`d under Cargo, which is where fixtures get regenerated.

### Ports and concurrency

These suites used to hard-code `9092`/`9093`, so two of them could not run at
once: the second to start lost the bind and reported `Address already in use` as
a test failure. Each test process allocates its own ports now, so any two suites
can run side by side.

The tests reach the broker over loopback; only the containers use the advertised
`host.docker.internal` name, which they resolve through
`--add-host=host.docker.internal:host-gateway`. CI additionally maps that name to
`127.0.0.1` in `/etc/hosts`, because a few host-side Rust clients bootstrap
through the advertised name rather than over loopback.

`jvm_acceptance` was the long pole at 552s: 48 tests in one binary, serialised
by that process's single port allocation. It is now eight `jvm_acceptance_*`
targets grouped by the cluster each one boots -- CLI round-trips, legacy 0.10
clients, durability, SASL, TLS, reassignment, quotas and tiered storage -- over
one harness in `tests/jvm_acceptance/mod.rs`. Each is its own process, so each
gets its own ports for free.

Splitting alone made CI *slower*, which is worth recording because the reasoning
looks sound until you measure it. The critical path did fall, 552s to 150s, but
the job went 852s to 1059s: these suites saturate a runner, so Bazel got about
1.2x concurrency both before and after, while eight binaries paid the image-load
and broker-boot cost eight times instead of once. Target granularity was never
the limit -- the box was.

The shard is therefore across runners, not within one. `docker-select` computes
the suite set once and emits a matrix, and each suite gets its own job, so the
wall clock is the slowest suite rather than the sum: 1179s to 355s end to end,
for about 2.7x the runner-minutes. Within a single binary the tests still share
that process's ports and run one at a time; per-test ports would lift that too.

### Docker Desktop hosts

The mixed-cluster suites advertise the bridge gateway that `docker network
inspect bridge` reports, so the host-run broker and the containers share one
address. That holds on native Linux Docker, where the gateway *is* the host.
Under Docker Desktop the containers sit inside a VM: `172.17.0.1` is the VM's
bridge, a host-bound port is unreachable at it, and only `host.docker.internal`
-- a different address again -- resolves. So `jvm_acceptance_*`, `jvm_features`
and `jvm_kip320_divergence` fail on such a host, with the JVM CLI reporting
`UnsupportedVersionException: The broker does not support CREATE_TOPICS`, which
is what it says when it cannot reach a broker at all.

To tell that apart from a real break: bind a host port, then from a container run
`nc -z 172.17.0.1 <port>`. Unreachable means Docker Desktop semantics. CI runs
native Linux Docker, so treat a local failure in these suites as environmental
until it reproduces there.

## Delivery

```
bazel run //packaging:image_load    # load into the local daemon
bazel run //packaging:push -- --tag dev  # push to ghcr.io
```

apko builds the broker base from locked Wolfi packages, including glibc and CA
certificates. rules_img adds the Bazel-built binary, loads it locally, and
pushes it. The image runs as `nonroot` (65532). The build uses no Dockerfile or
host package manager. `aspect delivery` skips the push when the image output did
not change.

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

The operator CLI spells this `krabka format`, as a subcommand.
That CLI also drives the gres layer, so it could not follow the broker out; the
command needs only the metadata and security crates, so it lives here as
`krabka-format` — a library as well as a binary, so `krabka-cli` can call it
rather than carry a second copy.

## Releases

A release of this repository is an annotated `vX.Y.Z` tag on `main`. Pushing
that tag builds the image, signs it, attests its SBOM and provenance, and
creates the GitHub release. [`CHANGELOG.md`](CHANGELOG.md) records what each tag
contains, and [releasing](docs/releasing.md) gives the commands that cut one.

The workspace is versioned and tagged as one unit, so no crate has a release of
its own and no crate changelog records one. `tools/lint/crate_changelogs.py`
holds that rule in CI. Each crate changelog keeps an `[Unreleased]` heading and
points at robot-head/crabka for the history before the extraction.

## Publishing

`krabka-log` and the other published names in this tree are released to crates.io
from [`robot-head/crabka`](https://github.com/robot-head/crabka). This repository
publishes no crate; consumers pin it by git revision.
