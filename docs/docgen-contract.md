# The crabka-docgen contract

`crabka-docgen` is the reference-page generator in
[`robot-head/crabka`](https://github.com/robot-head/crabka), under
`crates/docgen`. It links this broker as a library and renders three pages
from values it builds in process. It does not spawn the broker binary and it
does not read Rust source. This document names what it reads, so a change here
that breaks the tool fails a test in this repository first.

## Pinned revision

| Item | Value |
| :--- | :--- |
| Repository | `https://github.com/robot-head/crabka` |
| Crate | `crates/docgen` |
| Revision | `c412017b2ba63b325d985ecbb8fd7d18faed859a` |
| Committed | 2026-08-22 |

The pin is a record, not a dependency. This repository does not build the
tool, and `crabka-docgen` pins the broker by path inside its own workspace,
where the crates are still named `crabka-*`. Its source therefore spells the
entry points below as `crabka_broker::...` and `crabka_raft::scenarios`; this
document uses the `krabka_` names the code here carries. To move the pin, read
`crates/docgen/src/` at the new revision, compare the entry points below,
update this table, and update `crates/broker/tests/docgen_contract.rs` if a
shape changed.

## What the tool reads

### The `FileConfig` JSON Schema

`crates/docgen/src/broker.rs` calls `schemars::schema_for!(FileConfig)` and
hands the value to `render_sectioned_field_table` in `schema_md.rs`. That
renderer depends on this shape:

- The root is an object with `properties`, one per top-level TOML key, and a
  `$defs` table that every `$ref` points into as `#/$defs/<Name>`.
- An `Option<Struct>` field is `anyOf: [{$ref}, {type: "null"}]`. The renderer
  collapses it to the referenced struct.
- Each field carries its `///` comment as `description`, and a
  `#[serde(default)]` field carries `default`.
- A struct's `required` list names the fields with no default.

The same value is what `krabka-broker --print-config-schema` prints and what
[`config-schema.json`](config-schema.json) holds. The `format` annotations
`duration`, `byte-size`, and `ratio` on string fields are an addition the
renderer ignores; [`config-reference.md`](config-reference.md) uses them for
its units column.

### `api_catalog::supported_apis`

`protocol_apis_md` calls `krabka_broker::api_catalog::supported_apis()`,
sorts the result by `api_key`, and prints one row per entry with the name
`krabka_protocol::ApiKey::from_i16` gives it. The tool expects a non-empty
list with no repeated key, and a name for every key.

### `topic_config_docs`

`topic_configs_md` calls `krabka_broker::topic_config_docs()` and prints
`key`, `value_type`, `default`, `kip`, and `description` for each entry.

### `krabka_raft::scenarios`

`crates/docgen/src/scenarios.rs` runs `krabka_raft::scenarios::scenarios()`
and renders each `ScenarioTrace` into a Mermaid sequence diagram. It reads
`id`, `title`, `summary`, `invariant`, `nodes`, `steps`, and `outcome`, and
matches every `TraceAction` variant: `Deliver`, `Partition`, `Heal`, `Timeout`,
`Elected`, `Append`, and `Drop`. It special-cases the scenario whose `id` is
`split_brain_prevented`.

The module is `krabka_kraft_core::sim`, re-exported by `krabka-raft` under the
`scenarios` feature. No production build enables that feature;
`crates/broker/Cargo.toml` turns it on through a dev-dependency on
`krabka-raft`, so the broker's test build links the simulator and
`docgen_contract.rs` destructures every field and matches every variant above.
A rename fails to compile there before it fails in the tool.
`.cargo/mutants.toml` excludes `crates/kraft-core/src/sim.rs` from the mutation
sweep on the strength of this consumer.

## Where the contract is tested

`crates/broker/tests/docgen_contract.rs` asserts the schema shape, the
`supported_apis` invariants, the `topic_config_docs` columns, and the
`ScenarioTrace` shape, including that `scenarios()` returns
`split_brain_prevented`.
`crates/broker/tests/config_reference.rs` asserts that
[`config-schema.json`](config-schema.json) equals the schema the crate
generates and that [`config-reference.md`](config-reference.md) carries a row
for every key. `crates/broker/tests/example_broker_toml.rs` parses the example
configs and checks every key they set against the schema. The docs CI job
regenerates the reference page with `tools/generate-config-reference.py` and
diffs it.
