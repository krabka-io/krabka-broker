# Contributing to Krabka

Keep each change focused. Apache Kafka wire behavior, KIP semantics, and, from
1.0.0 on, the on-disk formats in
[`docs/persisted_formats.md`](docs/persisted_formats.md) are compatibility
requirements. Read [`CLAUDE.md`](CLAUDE.md) before you change a protocol or
persistence boundary.

## Build and Test

Bazel is the primary build and test path:

```sh
bazel build //...
bazel test //...
```

Cargo uses the same manifests and lockfile:

```sh
cargo nextest run --workspace
cargo test --workspace --doc
```

Behavior scenarios follow the [Gherkin style guide](docs/style_guides/gherkin_style_guide.md). It defines formulation, BRIEF, rules, capability folders, tags, journeys, and step definitions. Use `krabka_bdd::scenarios!` for new and migrated features, with the wrapper's step attributes and scenario-scoped `rstest` fixtures.

Group related examples under `Rule:` headings. The pinned macro patch includes rule scenarios in test discovery and preserves their backgrounds and tags. Check test discovery when you add or restructure a feature, and confirm that each example runs.

The [BDD observation guide](docs/style_guides/bdd_style_guide.md) supplements those conventions with a typed fixture example, independent expected values, asynchronous observations, and simulation scope. Use `krabka_bdd::Given<T>` for state slots and `Attempt<T, E>` for expected negative-path results. Then steps compare observations without repeating or changing the operation under test.

The wrapper awaits async steps on one runtime for the whole scenario and runs its after hooks on failure. Preserve Tokio runtime settings, platform conditions, external-service requirements, ignored tests, and cleanup when migrating scenarios. Add feature files and the crate's `Cargo.toml` to Bazel `compile_data`. Touch the Rust binding file when adding a feature to a directory the macro reads, so Cargo sees the new input.

Run `aspect check-gherkin` for feature changes. Remove resolved entries from the existing Gherkin baseline; fix new findings instead of adding exemptions. The Gherkin guide describes the wrapper's lint and runtime checks.

Add or update the smallest test that proves the change. Run the Docker suites
only when the change affects a container boundary:

```sh
bazel test --config=docker //crates/...
```

## Format and Lint

Run the repository format and lint gates before you submit a change:

```sh
bazel run //tools/format
bazel build --config=lint //...
cargo clippy --workspace --all-targets -- -D warnings
```

Do not add Clippy suppressions. Do not make a style-only sweep across files
that the change does not otherwise touch. The
[style guides](docs/style_guides/README.md) contain the code and documentation
rules.

### The `wasm32-wasip1` build

The broker library also builds for `wasm32-wasip1`, which the Cluster Lab runs in a browser. The `wasm` CI job runs Clippy for that target. To run it locally, install the target, unpack the [WASI sysroot](https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-25/wasi-sysroot-25.0.tar.gz) of wasi-sdk 25, and point the C compiler of the codecs at it:

```sh
rustup target add wasm32-wasip1
# Name the llvm-ar your distribution installs, for example llvm-ar-18.
export CC_wasm32_wasip1=clang AR_wasm32_wasip1=llvm-ar
export CFLAGS_wasm32_wasip1="--sysroot=/path/to/wasi-sysroot-25.0"
cargo clippy --target wasm32-wasip1 --lib -p krabka-broker -- -D warnings
```

`.cargo/config.toml` supplies the `--cfg tokio_unstable` that tokio needs for `net` on a wasm target. Keep native-only dependencies in a `[target.'cfg(not(target_family = "wasm"))'.dependencies]` table. A subsystem that needs one answers "unavailable on this platform" on wasm, as the OPA authorizer and the metrics server do.

That target has no threads either, so the broker runs its blocking work through `crate::blocking` rather than `tokio::task::spawn_blocking` or `block_in_place`, and takes its real-time timer from `time_util::system_timer` rather than `qubit_clock::StdTimer`. On wasm both run on the runtime's one thread. A test drives that path on a native target with `inline_blocking_on_this_thread`, as `tests/inline_blocking.rs` does.

## Bumping the upstream Kafka version

A Kafka version bump is two changes, one in each of two repositories. Give both
the same Kafka tag.

The schema sync, the protocol code regeneration and the JVM differential-test
oracle (`tools/oracle`, with its `kafka-clients` version) are in
[krabka-protocol](https://github.com/krabka-io/krabka-protocol). Follow the
procedure in that repository's `docs/CONTRIBUTING.md` first. Then, in this
repository:

1. Update the image tag and digest for the new release in `MODULE.bazel` and in
   [`bazel/images/BUILD.bazel`](bazel/images/BUILD.bazel), and the oracle line
   in [`docs/KIP_MATRIX.md`](docs/KIP_MATRIX.md). `aspect check-images` holds
   the first two in step.
2. Run `bazel test --config=docker //crates/...`.
3. Commit the image pins.

## Benchmarks

`cargo bench -p krabka-broker` and `cargo bench -p krabka-log` are the
microbenchmarks. Bazel's `crate_bench` macro compiles and lints the same suites;
`bazel run //crates/log:log_bench` runs the storage suite on demand.

[`bench/`](bench/README.md) is the cluster harness. It runs
`krabka-bench-driver` as a Kubernetes Job against a Krabka cluster and a
Strimzi cluster in turn, and aggregates the per-run JSON into one report. It
needs a live cluster and a kubeconfig, so no CI job runs it; read that
directory's README before you start one.

## Profiling

Two examples of `krabka-broker` make a CPU profile of the broker alone. Run
them in two shells:

```sh
cargo run --release -p krabka-broker --example profile_server
cargo run --release -p krabka-broker --example loadgen
```

`profile_server` boots one broker on `127.0.0.1:9092` and prints its process
id. Attach `perf record -F 999 -g -p <pid>` to that id. `loadgen` makes the
traffic from a second process, so no client work reaches the profile. Each
example's module comment lists the environment variables it reads.

## Special Test Tiers

### Turmoil KRaft tests

The `kraft_turmoil` suite runs three real async KRaft controllers with [Turmoil](https://github.com/tokio-rs/turmoil) TCP sockets and virtual Tokio time. It checks election and metadata replication, minority-write refusal during a partition, recovery after the partition heals, and leader crash/restart. The outline rows use fixed seeds 1, 7, and 42 from [`kraft_turmoil.feature`](crates/raft/tests/features/kraft_turmoil/kraft_turmoil.feature). Add rows there to exercise more network schedules.

```sh
bazel test //crates/raft:kraft_turmoil_test
cargo test --locked -p krabka-raft --test kraft_turmoil
```

The suite is included in the normal Bazel and Cargo test discovery. It needs no Docker daemon or external broker. Its synchronous wrapper steps drive Turmoil's own runtimes; keep those steps synchronous to avoid a nested Tokio `block_on`. For a replay that also seeds Tokio's task scheduling and `select!` decisions, compile with Tokio's unstable test support:

```sh
RUSTFLAGS="--cfg tokio_unstable" cargo test --locked -p krabka-raft --test kraft_turmoil
```

Turmoil controls the network and timers. The controllers use real temporary directories, and a restarted host opens the same directory. These tests cover recovery of acknowledged writes; they do not simulate torn writes, disk failures, TLS, or the production Kafka connection protocol.

### External services and mutation testing

The ignored integration tests need their external service or Kafka oracle.
Run the applicable test with `-- --ignored`. The full Bazel Docker lane is the
preferred check for container-backed tests.

Run mutation sweeps locally for `raft`, `kraft-core`, `log`, `verified`,
`throttle`, `audit`, `authz`, and `broker`. The scheduled `mutants` workflow
has been removed. [`docs/mutants-baseline.md`](docs/mutants-baseline.md)
keeps its historical results. To sweep one crate, run its whole sweep in
one shard:

```sh
aspect mutants-shard --target //crates/raft:raft_mutants
```

`--index` and `--count` split that sweep into shards,
so `--index 0 --count 8` runs the first eighth of it. Either way a mutant that
survives every test fails the shard, because `.cargo/mutants.toml` admits no
survivor baseline.

Changes to a Creusot kernel or contract also need the commands in the
[verification ledger](docs/verification.md).

The full set of container suites, the `gssapi` lane, the `jepsen` lane (see
[Jepsen Kafka workload](docs/jepsen.md)), and the external link check run at
full width only on the nightly schedule, where they gate no merge, so `ci.yml`
reports them instead: a scheduled run in which any of those lanes fails opens
an issue labelled `nightly-red` -- naming the failed jobs and linking the run
-- or comments on the open one if there already is one, and the next green
scheduled run closes it. A skipped lane is not a failure, and a cancelled run
reports nothing either way.

Link checking is split along the same line: a pull request runs `lychee
--offline`, which resolves file-relative links and never touches the network,
while the nightly `links` lane runs it without `--offline` to fetch the
external URLs -- KIP pages on `cwiki.apache.org`, `kafka.apache.org` paths,
`docs.rs` items -- with the run's token authenticating the `github.com` links
and the compose-internal example hostnames excluded. A link that is correct but
permanently unreachable to a checker goes in [`.lycheeignore`](.lycheeignore)
with a line saying why; anything else it reports is a link to fix.

## Duplicate Code

Run `aspect check-cpd` before submitting Rust refactors. The required CPD job
uses **jscpd 5.4.1** at **52 tokens / 5 lines**, ignoring whitespace and comments.
Every duplicate fails; there is no baseline allowance or suppression list.
The scan includes all tracked and non-ignored untracked Rust sources, including
tests and hidden directories, copied into an isolated inventory.

The task requires Git, Node.js 18 or newer, and npm. `npm ci` verifies the pinned
detector and its platform binary against the committed lockfile. Reports and
the source inventory go to `.cpd/`; CI retains these as the `cpd` artifact.
See the [checker documentation](tools/cpd/README.md) for details.

## Submit a Change

Before you open a pull request:

1. Run the build, tests, format check, and lint checks that apply to the change.
2. Update documentation when the public behavior or a caller precondition changes.
3. Record a change that an operator can see under `[Unreleased]` in the root
   [`CHANGELOG.md`](CHANGELOG.md). Krabka releases the workspace as one unit, so
   a crate changelog records no release and CI fails when one does. The
   [release process](docs/releasing.md) turns those entries into a tag.
4. State what changed and list the checks that you ran. The
   [pull request template](.github/pull_request_template.md) lists the gates
   as a checklist, and [`CODEOWNERS`](.github/CODEOWNERS) names the reviewers.

Report a vulnerability as [`SECURITY.md`](SECURITY.md) describes, not in a
public issue.
