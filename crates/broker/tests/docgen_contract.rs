//! The shape `krabka-docgen` consumes from this crate.
//!
//! `docs/docgen-contract.md` names the tool, its pinned revision, and the
//! entry points it reads in process. Each one is asserted below. The
//! `krabka_raft::scenarios` simulator sits behind the `scenarios` feature,
//! which this crate's dev-dependency on `krabka-raft` turns on for the test
//! build alone.
#![cfg(test)]

use {
    assert2::{assert, check},
    krabka_broker::file_config,
    krabka_protocol::owned::api_versions_response::ApiVersion,
    krabka_raft::scenarios::{NodeRole, ScenarioTrace, TraceAction, TraceStep, scenarios},
    std::collections::BTreeSet,
    support::api_versions::supported_client_apis,
};

mod support;

#[cfg(test)]
#[rstest_bdd_macros::then("file config schema has the shape docgen renders")]
fn file_config_schema_has_the_shape_docgen_renders(reference: &DocgenReference) {
    let schema = reference.schema.as_ref().expect("schema generated");
    // `render_sectioned_field_table` partitions `properties` of the root and
    // resolves `$ref` pointers into a `$defs` table.
    check!(schema["title"] == "FileConfig");
    check!(schema["type"] == "object");
    let properties = schema["properties"]
        .as_object()
        .expect("root schema has properties");
    check!(!properties.is_empty());
    let defs = schema["$defs"].as_object().expect("root schema has $defs");
    check!(defs.contains_key("RuntimeFileConfig"));
    check!(defs.contains_key("FileListener"));
    check!(defs.contains_key("FileTlsConfig"));

    // A nested table is an `anyOf` over a local `$ref` and `null`, which is
    // the shape the renderer's `effective_schema` collapses.
    let branches = properties["tls_config"]["anyOf"]
        .as_array()
        .expect("tls_config is an Option<struct>");
    let reference = branches
        .iter()
        .find_map(|branch| branch["$ref"].as_str())
        .expect("one branch is a $ref");
    assert!(reference == "#/$defs/FileTlsConfig");
    check!(branches.iter().any(|branch| branch["type"] == "null"));

    // The renderer reads `description` for the blurb of each section, so the
    // doc comment must survive into the schema.
    let description = properties["controller_quorum_voters"]["description"]
        .as_str()
        .expect("controller_quorum_voters carries its doc comment");
    check!(description.contains("KIP-595"));
}

#[cfg(test)]
#[rstest_bdd_macros::then("supported apis is non empty and unique by key")]
fn supported_apis_is_non_empty_and_unique_by_key(reference: &DocgenReference) {
    let apis = reference.apis.as_ref().expect("client APIs discovered");
    assert!(!apis.is_empty());
    let keys: BTreeSet<i16> = apis.iter().map(|api| api.api_key).collect();
    assert!(
        keys.len() == apis.len(),
        "supported_apis repeats an api_key; docgen renders one row per key"
    );
    // Sorting by key is what the tool does before it renders, so the sort
    // must be total: no two entries may compare equal.
    let mut sorted = apis.clone();
    sorted.sort_by_key(|api| api.api_key);
    let sorted_keys: Vec<i16> = sorted.iter().map(|api| api.api_key).collect();
    assert!(sorted_keys.windows(2).all(|pair| pair[0] < pair[1]));
    // ApiVersions itself is advertised, and every advertised key has a name.
    check!(keys.contains(&18));
    for api in apis {
        check!(
            krabka_protocol::ApiKey::from_i16(api.api_key).is_some(),
            "api_key {} has no ApiKey name; docgen would print `?`",
            api.api_key
        );
        check!(
            api.min_version <= api.max_version,
            "api_key {} advertises an empty version range",
            api.api_key
        );
    }
}

#[cfg(test)]
#[rstest_bdd_macros::then("topic config docs carry the columns docgen prints")]
fn topic_config_docs_carry_the_columns_docgen_prints(reference: &DocgenReference) {
    let docs = reference.docs.as_ref().expect("topic reference generated");
    assert!(!docs.is_empty());
    let keys: BTreeSet<&str> = docs.iter().map(|doc| doc.key).collect();
    assert!(keys.len() == docs.len(), "topic_config_docs repeats a key");
    check!(keys.contains("retention.ms"));
    for doc in docs {
        check!(!doc.value_type.is_empty(), "{} has no value type", doc.key);
        check!(
            !doc.description.is_empty(),
            "{} has no description",
            doc.key
        );
    }
}

/// The one scenario `krabka-docgen` special-cases by id.
const SPECIAL_CASED_SCENARIO: &str = "split_brain_prevented";

#[cfg(test)]
#[rstest_bdd_macros::then("scenarios record the trace shape docgen renders")]
fn scenarios_record_the_trace_shape_docgen_renders(reference: &DocgenReference) {
    let traces = reference.traces.as_ref().expect("simulations executed");
    assert!(!traces.is_empty());
    let ids: Vec<&str> = traces.iter().map(|trace| trace.id.as_str()).collect();
    check!(
        ids.contains(&SPECIAL_CASED_SCENARIO),
        "docgen looks up the scenario with id {SPECIAL_CASED_SCENARIO:?}; found {ids:?}"
    );
    let unique: BTreeSet<&str> = ids.iter().copied().collect();
    check!(unique.len() == ids.len(), "docgen keys its slides by id");

    for trace in traces {
        // Destructured in full so that a renamed or removed field fails to
        // compile here, and not first in the tool.
        let ScenarioTrace {
            id,
            title,
            summary,
            invariant,
            nodes,
            steps,
            outcome,
        } = trace;
        for (name, text) in [
            ("title", title),
            ("summary", summary),
            ("invariant", invariant),
            ("outcome", outcome),
        ] {
            check!(!text.is_empty(), "scenario {id} has an empty {name}");
        }
        check!(!nodes.is_empty(), "scenario {id} names no participants");
        check!(!steps.is_empty(), "scenario {id} records no steps");

        for (position, step) in steps.iter().enumerate() {
            let TraceStep {
                index,
                clock_ms: _clock_ms,
                action,
                note: _note,
                roles,
            } = step;
            check!(
                *index == position,
                "scenario {id} numbers its steps in order"
            );
            check!(
                roles.len() == nodes.len(),
                "scenario {id} step {position} snapshots every node"
            );
            for role in roles {
                let NodeRole {
                    id: node,
                    role: _role,
                    epoch: _epoch,
                    log_len: _log_len,
                    hwm: _hwm,
                    partitioned: _partitioned,
                } = role;
                check!(nodes.contains(node), "scenario {id} snapshots node {node}");
            }
            // One arm per variant and no wildcard: a new or renamed variant is
            // a compile error, which is what docgen's exhaustive match hits.
            let participants: Vec<u64> = match action {
                TraceAction::Deliver { src, dst, event }
                | TraceAction::Drop { src, dst, event } => {
                    check!(
                        !event.is_empty(),
                        "scenario {id} step {position} labels its message"
                    );
                    vec![*src, *dst]
                }
                TraceAction::Partition { node }
                | TraceAction::Heal { node }
                | TraceAction::Timeout { node, .. }
                | TraceAction::Elected { node, .. }
                | TraceAction::Append { node, .. } => vec![*node],
            };
            for participant in participants {
                check!(
                    nodes.contains(&participant),
                    "scenario {id} step {position} names node {participant}, not a participant"
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ReferenceSource {
    Configuration,
    ClientApis,
    TopicConfiguration,
    RaftSimulation,
}
struct DocgenReference {
    source: Option<ReferenceSource>,
    schema: Option<serde_json::Value>,
    apis: Option<Vec<ApiVersion>>,
    docs: Option<Vec<krabka_broker::TopicConfigDoc>>,
    traces: Option<Vec<ScenarioTrace>>,
}
#[rstest::fixture]
fn reference() -> DocgenReference {
    // Start this scenario with fresh inputs and observations.

    DocgenReference {
        source: None,
        schema: None,
        apis: None,
        docs: None,
        traces: None,
    }
}
#[rstest_bdd_macros::given("docgen reads the {source} reference source")]
fn select_reference_source(reference: &mut DocgenReference, source: String) {
    reference.source = Some(match source.as_str() {
        "configuration schema" => ReferenceSource::Configuration,
        "client API catalog" => ReferenceSource::ClientApis,
        "topic configuration registry" => ReferenceSource::TopicConfiguration,
        "Raft simulation catalog" => ReferenceSource::RaftSimulation,
        other => panic!("unknown documentation source {other}"),
    });
}
#[rstest_bdd_macros::when(
    "docgen obtains the selected reference data through its public entry point"
)]
fn read_reference_source(reference: &mut DocgenReference) {
    match reference.source.expect("documentation source selected") {
        ReferenceSource::Configuration => reference.schema = Some(file_config::config_schema()),
        ReferenceSource::ClientApis => reference.apis = Some(supported_client_apis()),
        ReferenceSource::TopicConfiguration => {
            reference.docs = Some(krabka_broker::topic_config_docs());
        }
        ReferenceSource::RaftSimulation => reference.traces = Some(scenarios()),
    }
}
rstest_bdd_macros::scenarios!("tests/features/migrated/tests/docgen_contract.feature", fixtures = [reference: DocgenReference]);
