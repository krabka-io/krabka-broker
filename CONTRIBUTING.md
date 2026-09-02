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

## Special Test Tiers

The ignored integration tests need their external service or Kafka oracle.
Run the applicable test with `-- --ignored`. The full Bazel Docker lane is the
preferred check for container-backed tests.

The mutation gate for the consensus engine is:

```sh
bash tools/check-mutants.sh //crates/raft:raft_mutants
```

Changes to a Creusot kernel or contract also need the commands in the
[verification ledger](docs/verification.md).

## Submit a Change

Before you open a pull request:

1. Run the build, tests, format check, and lint checks that apply to the change.
2. Update documentation when the public behavior or a caller precondition changes.
3. Record a change that an operator can see under `[Unreleased]` in the root
   [`CHANGELOG.md`](CHANGELOG.md). Krabka releases the workspace as one unit, so
   a crate changelog records no release and CI fails when one does. The
   [release process](docs/releasing.md) turns those entries into a tag.
4. State what changed and list the checks that you ran.
