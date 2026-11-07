# krabka-bdd-model

The Gherkin model behind [`krabka-bdd`](../bdd/README.md): loading, rule flattening, outline expansion, naming, the tag glossary and the house-style lint.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

Plain functions over parsed `.feature` files, so the `krabka-bdd-macros` proc macros and their unit tests share one implementation. `plan` flattens each feature into the scenarios a test binary runs, Backgrounds and Examples rows included, and names their tests. `lint` checks a feature against the [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md#lint-rules), with the tags that [`glossary.toml`](glossary.toml) defines. `baseline` is the ratchet that lets the lint run over older features without failing on what they already break. `cucumber` expands Cucumber expressions into rstest-bdd step patterns. The crate is not published.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
