# krabka-bdd-cli

The `krabka-bdd` command-line tool: the house-style Gherkin lint with a ratchet baseline, example maps, a living-documentation index and the glossary report.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Quick Start

```sh
cargo run -q -p krabka-bdd-cli -- lint --baseline crates/bdd-cli/gherkin-baseline.tsv crates
bazel run //crates/bdd-cli:krabka-bdd -- map crates/log/tests/features/roll
```

Each `PATH` is a `.feature` file or a directory searched for them, skipping hidden directories and `target`. Relative paths are taken from the current directory, or from the workspace root when `bazel run` sets `BUILD_WORKSPACE_DIRECTORY`, and output shows paths relative to that directory.

## Commands

| Command | Output | Exit |
| :--- | :--- | :--- |
| `lint [--baseline FILE] [--write-baseline] PATH...` | Every finding as `path:line:col: rule-id: message. Fix: hint`, advisories marked `(advisory)`, then a summary line. With `--baseline`, the `new:` findings and the `fixed, remove from the baseline:` entries. | 1 on an error finding; with `--baseline`, 1 on a new error or a fixed entry instead. Advisories never fail. |
| `lint --baseline FILE --write-baseline PATH...` | The findings, then writes the error findings to `FILE`. | 0 |
| `map PATH...` | Each feature as a Markdown example map: 🟨 feature, 🟦 rules, 🟩 examples and outline rows, 🟥 open questions, with counts and readiness notes. | 1 when a file does not parse |
| `docs [--out-dir DIR] PATH...` | A Markdown index grouped by capability folder, with rules, scenarios, tags and journeys, linked relative to `DIR`. | 1 when a file does not parse |
| `glossary [--min-steps N] PATH...` | Candidate domain terms that at least `N` steps use (default 3) and the glossary lacks, and glossary terms no step uses. | 0 |
| `tags-table` | The tag table of the [Gherkin style guide](../../docs/style_guides/gherkin_style_guide.md#tags), rendered from [`glossary.toml`](../bdd-model/glossary.toml). | 0 |

Every command exits 2 on a usage or IO error.

The lint sees each file by its path relative to the nearest directory with a `Cargo.toml`, as `krabka_bdd::scenarios!` does, and resolves `Depends on:` paths there. A baseline entry names the file by the shown path, so [`gherkin-baseline.tsv`](gherkin-baseline.tsv) covers every crate from the repository root. The rules and the baseline format come from [`krabka-bdd-model`](../bdd-model/README.md). The crate is not published.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
