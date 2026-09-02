<!--
State what changed and why. Link the issue. List the checks you ran.
CONTRIBUTING.md describes each gate.
-->

## What changed

## Why

## Checks

- [ ] `bazel test //...` passes, or the run is linked below.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes with no new `#[allow(clippy::...)]`.
- [ ] `bazel run //tools/format` (or `aspect format`) left the tree unchanged.
- [ ] Tests use `assert2::assert!` and exercise behavior, not source text.
- [ ] No backwards-compatibility shim: no `#[serde(default)]` for old logs, no kept `V1` variant, no default-off feature flag, no migration code.
- [ ] Kafka wire-protocol byte exactness and KIP semantics are unchanged, or the change matches the latest released cp-kafka image and says so.
- [ ] Documentation follows the [style guides](../docs/style_guides/README.md) and is updated where public behavior or a caller precondition changed.
- [ ] A Creusot contract or a Stateright model changed: the [verification catalog](../docs/verification.md) is updated and the proofs were re-run.
- [ ] A container suite is affected: `bazel test --config=docker //crates/...` was run for it.
