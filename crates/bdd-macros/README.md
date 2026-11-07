# krabka-bdd-macros

The proc macros behind [`krabka-bdd`](../bdd/README.md): `scenarios!` and `#[derive(StepParam)]`.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

`scenarios!` parses every `.feature` file under a directory with the `gherkin` crate, lints it against the [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md#krabka-bdd), and expands to `rstest_bdd_macros::scenarios!` plus a step-registry check. `#[derive(StepParam)]` generates `FromStr` and `Display` for a unit-variant enum. The lint and the feature model are plain functions in private modules, so their unit tests call them directly. Use the macros through `krabka-bdd`, which re-exports them next to the runtime types their expansions name. The crate is built on [`moxy`](https://crates.io/crates/moxy) and is not published.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
