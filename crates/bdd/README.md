# krabka-bdd

Krabka's BDD layer over rstest-bdd: it lints Gherkin features against the house style, checks every step against the step registry, and adds the helpers step definitions share.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

Krabka's crates illustrate Kafka behavior with Gherkin scenarios. rstest-bdd keeps the step registry and the step definitions; `krabka-bdd` reads the features, generates one test per scenario and per Examples row, and runs the steps on one Tokio runtime per scenario. It is a dev-dependency only and is not published. The macros live in [`krabka-bdd-macros`](../bdd-macros/README.md) and the feature model in [`krabka-bdd-model`](../bdd-model/README.md).

## Features

- `scenarios!` — lints every feature under a directory at compile time against the [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md#krabka-bdd), then generates the tests. `Rule:` scenarios run, rows are named after their `case` cell, `@formulated` and `@manual` scenarios are ignored, and `@inprogress` ones are expected to fail.
- The runner: `async` steps that really await, strict resolution of undefined and ambiguous steps, a `World` and tag-scoped `#[before]` / `#[after]` hooks that always run, and one diagnostic per failure naming the feature line, rule, Examples row and step.
- A registry check per test binary: undefined, ambiguous and journey-only steps fail; unused step definitions are listed.
- `runtime = "tokio-multi-thread"` — worker threads, so a task spawned in a Given keeps running between steps.
- `#[given]`, `#[when]`, `#[then]`, `#[step]` — Cucumber expressions, several expressions per step, and checks on placeholder order and return type.
- `Attempt<T, E>` — the result of an "attempts to" step, for a later Then to check.
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

#[krabka_bdd::given("a log has cleanup policy {policy}")]
fn log_has_cleanup_policy(world: &mut World, policy: CleanupPolicy) {
    world.policy.set(policy);
}

krabka_bdd::scenarios!("tests/features/log_policy", fixtures = [world: World]);
```

A crate that uses it lists `krabka-bdd`, `rstest`, `rstest-bdd` and `rstest-bdd-macros` as dev-dependencies. A published crate gives `krabka-bdd` a `path` and no `version`, so `cargo publish` drops it.

## Documentation

- [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md): the house style, the lint rule ids and their fixes.
- The crate-level rustdoc lists every `scenarios!` argument.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
