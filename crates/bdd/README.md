# krabka-bdd

Krabka's BDD layer over rstest-bdd: it lints Gherkin features against the house style, checks every step against the step registry, and adds the helpers step definitions share.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

Krabka's crates illustrate Kafka behavior with Gherkin scenarios that run through [rstest-bdd](https://docs.rs/rstest-bdd). `krabka-bdd` replaces the `rstest_bdd_macros::scenarios!` call that binds a feature directory to its step definitions. It is a dev-dependency only and is not published. The macros live in [`krabka-bdd-macros`](../bdd-macros/README.md).

## Features

- `scenarios!` — lints every feature under a directory at compile time against the [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md#krabka-bdd), then runs it with rstest-bdd.
- A generated test per `scenarios!` call that fails on undefined and ambiguous steps, naming each matching definition's `file:line`.
- `runtime = "tokio-multi-thread"` — runs each scenario on a multi-thread Tokio runtime, so a task spawned in a Given keeps running between steps.
- `#[derive(StepParam)]` — `FromStr` and `Display` over the step words of a unit-variant enum.
- `Given<T>` — a scenario-state slot whose unset read names the step that should have filled it.

## Usage

```rust,ignore
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum CleanupPolicy {
    Delete,
    Compact,
    #[step_param("compact,delete")]
    CompactAndDelete,
}

struct World {
    policy: krabka_bdd::Given<CleanupPolicy>,
}

#[rstest::fixture]
fn world() -> World {
    World { policy: krabka_bdd::Given::new("policy", "Given a log has cleanup policy <policy>") }
}

#[rstest_bdd_macros::given("a log has cleanup policy {policy}")]
fn log_has_cleanup_policy(world: &mut World, policy: CleanupPolicy) {
    world.policy.set(policy);
}

krabka_bdd::scenarios!("tests/features/log_policy", fixtures = [world: World]);
```

A crate that uses it lists `krabka-bdd`, `rstest`, `rstest-bdd` and `rstest-bdd-macros` as dev-dependencies, and also `rstest-bdd-harness` when it uses the multi-thread runtime. A published crate gives `krabka-bdd` a `path` and no `version`, so `cargo publish` drops it.

## Documentation

- [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md): the house style, the lint rule ids and their fixes.
- The crate-level rustdoc lists every `scenarios!` argument.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
