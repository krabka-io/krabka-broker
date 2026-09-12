# Contributing to Krabka

Keep each change focused. Krabka is greenfield, but Apache Kafka wire behavior
and KIP semantics are compatibility requirements. Read
[`CLAUDE.md`](CLAUDE.md) before you change a protocol or persistence boundary.

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

## JVM Oracle

[`tools/oracle`](tools/oracle/README.md) is the JVM differential-test oracle.
It answers Kafka wire questions with Apache Kafka's own `kafka-clients` code.
You need a JDK 17 to build it. You do not need a system Gradle.

```sh
(cd tools/oracle && ./gradlew installDist)
```

Gradle installs the start script at
`tools/oracle/build/install/krabka-oracle/bin/krabka-oracle`. The wire-level
differential suites that drive it live in
[krabka-protocol](https://github.com/krabka-io/krabka-protocol), not here. The
`oracle` job of `ci.yml` builds the oracle on every pull request, so a
`kafka-clients` bump that does not compile fails here.

## Bumping the upstream Kafka version

A Kafka version bump is two changes, one in each of two repositories. Give both
the same Kafka tag.

The schema sync and the protocol code regeneration are the other half. They
belong to
[krabka-protocol](https://github.com/krabka-io/krabka-protocol), in that
repository's `docs/CONTRIBUTING.md`. Do that half first. Then, in this
repository:

1. Update the image tag and digest for the new release in `MODULE.bazel` and in
   [`bazel/images/BUILD.bazel`](bazel/images/BUILD.bazel), and the oracle line
   in [`docs/KIP_MATRIX.md`](docs/KIP_MATRIX.md). `aspect check-images` holds
   the first two in step.
2. Update the `kafka-clients` version in
   [`tools/oracle/build.gradle.kts`](tools/oracle/build.gradle.kts) to the same
   release.
3. Run `(cd tools/oracle && ./gradlew installDist)`.
4. Run `bazel test --config=docker //crates/...`.
5. Commit the image pins and the Gradle bump together.

## Benchmarks

`cargo bench -p krabka-broker` and `cargo bench -p krabka-log` are the
microbenchmarks. There is no `crate_bench` rule, so Cargo runs them.

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

The ignored integration tests need their external service or Kafka oracle.
Run the applicable test with `-- --ignored`. The full Bazel Docker lane is the
preferred check for container-backed tests.

The mutation gate of record is the scheduled `mutants` workflow, not a local
command: it sweeps `raft`, `kraft-core`, `log`, `verified`, `throttle`,
`audit`, `authz`, and `broker` every Sunday, and
[`docs/mutants-baseline.md`](docs/mutants-baseline.md) records when each of
those crates last came back green. To sweep one crate yourself -- the
consensus engine, say -- run its whole sweep in one shard:

```sh
aspect mutants-shard --target //crates/raft:raft_mutants
```

`--index` and `--count` split that sweep the way the workflow's matrix does,
so `--index 0 --count 8` runs the first eighth of it. Either way a mutant that
survives every test fails the shard, because `.cargo/mutants.toml` admits no
survivor baseline.

Changes to a Creusot kernel or contract also need the commands in the
[verification ledger](docs/verification.md).

The container matrix, the `gssapi` lane, and the external link check run at
full width only on the nightly schedule, where they gate no merge, so `ci.yml`
reports them instead: a scheduled run in which any of those lanes fails opens
an issue labelled `nightly-red` -- naming the failed jobs and linking the run
-- or comments on the open one if there already is one, and the next green
scheduled run closes it. A skipped lane is not a failure, and a cancelled run
reports nothing either way.

The mutation sweeps report the same way and against the same `nightly-red`
label, from `mutants.yml` rather than `ci.yml`: a `needs` cannot cross
workflows, so the sweep's `report` job files and closes that one issue itself.
It also uploads each shard's log as `mutants-<crate>-<shard>`, so a survivor
can be read without rerunning the sweep, and rewrites the last-green table in
[`docs/mutants-baseline.md`](docs/mutants-baseline.md).

Link checking is split along the same line: a pull request runs `lychee
--offline`, which resolves file-relative links and never touches the network,
while the nightly `links` lane runs it without `--offline` to fetch the
external URLs -- KIP pages on `cwiki.apache.org`, `kafka.apache.org` paths,
`docs.rs` items -- with the run's token authenticating the `github.com` links
and the compose-internal example hostnames excluded. A link that is correct but
permanently unreachable to a checker goes in [`.lycheeignore`](.lycheeignore)
with a line saying why; anything else it reports is a link to fix.

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
