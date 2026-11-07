# krabka-bdd-macros

The proc macros behind [`krabka-bdd`](../bdd/README.md): `scenarios!`, the step and hook attributes, and `#[derive(StepParam)]`.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

`scenarios!` lints every `.feature` file under a directory and generates one `#[tokio::test]` per scenario and per Examples row, which hands a static scenario description and its fixtures to the krabka-bdd runner. `#[given]`, `#[when]`, `#[then]` and `#[step]` expand Cucumber expressions, check a step's placeholders and return type, and re-emit rstest-bdd's own step attributes. `#[before]` and `#[after]` register tag-scoped scenario hooks. `#[derive(StepParam)]` generates `FromStr` and `Display` for a unit-variant enum. The decisions are plain functions over [`krabka-bdd-model`](../bdd-model/README.md), so unit tests call them directly. Use the macros through `krabka-bdd`, which re-exports them next to the runtime types their expansions name. The crate is built on [`moxy`](https://crates.io/crates/moxy) and is not published.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
