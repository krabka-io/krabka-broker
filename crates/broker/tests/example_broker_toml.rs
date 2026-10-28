//! The example configs under `docs/examples/` parse and apply.
//!
//! Each file is read twice. As written, it goes through `toml` into
//! `FileConfig` and then through `FileConfig::apply_to` onto a default
//! `BrokerConfig`, which is the path the binary takes under `--config-file`.
//! Then every disabled setting, a line that starts with `#` and no space, is
//! turned on and the result is parsed again, so a key that is commented out
//! because it needs a key file or a service still has to be a key the broker
//! knows. Apply is skipped on that second pass because those settings read
//! files at apply time.
//!
//! `FileConfig` tolerates unknown keys, so a key an example spells wrong is
//! still a file that parses. The last test therefore walks each example as
//! plain TOML and checks every key, at every depth, against
//! `docs/config-schema.json`, which is the schema `FileConfig` generates.
#![cfg(test)]

use {
    assert2::{assert, check},
    krabka_broker::{config::BrokerConfig, file_config::FileConfig},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    serde_json::Value as Json,
    toml::Value as Toml,
};

const EXAMPLES: [&str; 2] = [
    "docs/examples/broker-single-node.toml",
    "docs/examples/broker-three-node-quorum.toml",
];

#[path = "support/repository.rs"]
mod repository;

use repository::read_text as read_example;

/// Turn every disabled setting on. Prose comments start with `# ` and stay.
fn enable_disabled_settings(text: &str) -> String {
    text.lines()
        .map(|line| match line.strip_prefix('#') {
            Some(rest) if !rest.is_empty() && !rest.starts_with([' ', '#']) => rest,
            _ => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse(relative: &str, text: &str) -> FileConfig {
    toml::from_str(text).unwrap_or_else(|error| panic!("{relative} does not parse: {error}"))
}

use repository::checked_in_schema;

/// The object schemas a node can stand for, with `$ref`, `anyOf` and `oneOf`
/// resolved. `null` branches are dropped: an absent table is not a key.
fn object_branches<'a>(root: &'a Json, node: &'a Json, out: &mut Vec<&'a Json>) {
    if let Some(reference) = node.get("$ref").and_then(Json::as_str) {
        let name = reference
            .strip_prefix("#/$defs/")
            .expect("schemars emits local $defs refs");
        object_branches(root, &root["$defs"][name], out);
        return;
    }
    let branches = ["anyOf", "oneOf"]
        .into_iter()
        .filter_map(|key| node.get(key).and_then(Json::as_array))
        .flatten();
    let mut nested = false;
    for branch in branches {
        nested = true;
        object_branches(root, branch, out);
    }
    if !nested && node["type"] != "null" {
        out.push(node);
    }
}

/// A property name the schema knows, and the schema that describes it.
struct KnownProperty<'a> {
    name: &'a String,
    schema: &'a Json,
}

/// Check every key of `table` against the schema `node` describes, then
/// descend into nested tables and arrays of tables.
fn check_table_keys(root: &Json, node: &Json, table: &toml::Table, path: &str) {
    let mut branches = Vec::new();
    object_branches(root, node, &mut branches);
    // A map with free-form keys, such as `server_properties`, has an
    // `additionalProperties` schema and no fixed `properties`; its keys are
    // the operator's to choose.
    if branches.iter().any(|branch| {
        branch
            .get("additionalProperties")
            .is_some_and(Json::is_object)
    }) {
        return;
    }
    let known: Vec<KnownProperty<'_>> = branches
        .iter()
        .filter_map(|branch| branch.get("properties").and_then(Json::as_object))
        .flat_map(|properties| properties.iter())
        .map(|(name, schema)| KnownProperty { name, schema })
        .collect();
    assert!(
        !known.is_empty(),
        "{path} is a table in the example but not an object in the schema"
    );
    for (key, value) in table {
        let field = known
            .iter()
            .find(|property| property.name == key)
            .map(|property| property.schema);
        let Some(field) = field else {
            panic!("{path}.{key} is not a key docs/config-schema.json knows");
        };
        let child = format!("{path}.{key}");
        match value {
            Toml::Table(nested) => check_table_keys(root, field, nested, &child),
            Toml::Array(items) => {
                let mut item_schemas = Vec::new();
                object_branches(root, field, &mut item_schemas);
                for item_schema in item_schemas {
                    let Some(items_node) = item_schema.get("items") else {
                        continue;
                    };
                    for (position, item) in items.iter().enumerate() {
                        if let Toml::Table(nested) = item {
                            check_table_keys(
                                root,
                                items_node,
                                nested,
                                &format!("{child}[{position}]"),
                            );
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

struct ExampleDocument {
    relative: &'static str,
    text: String,
    enabled: String,
    file: Option<FileConfig>,
    enabled_file: Option<FileConfig>,
    applied: Option<BrokerConfig>,
    table: Option<toml::Table>,
}

#[derive(Default)]
struct OperatorExamples {
    documents: Vec<ExampleDocument>,
    schema: Option<Json>,
}

#[fixture]
fn examples() -> OperatorExamples {
    // Start this scenario with fresh inputs and observations.

    OperatorExamples {
        documents: Vec::new(),
        schema: None,
    }
}

fn load_examples(examples: &mut OperatorExamples, paths: &[&'static str]) {
    examples.schema = Some(checked_in_schema());
    examples.documents = paths
        .iter()
        .map(|&relative| ExampleDocument {
            relative,
            text: read_example(relative),
            enabled: String::new(),
            file: None,
            enabled_file: None,
            applied: None,
            table: None,
        })
        .collect();
}

#[given("the checked-in single-node and three-node broker examples")]
fn documented_examples(examples: &mut OperatorExamples) { load_examples(examples, &EXAMPLES); }

#[given("the checked-in single-node broker example")]
fn single_node_example(examples: &mut OperatorExamples) { load_examples(examples, &EXAMPLES[..1]); }

#[given("the checked-in three-node broker example")]
fn three_node_example(examples: &mut OperatorExamples) { load_examples(examples, &EXAMPLES[1..]); }

#[when("the examples are parsed as FileConfig")]
fn parse_examples(examples: &mut OperatorExamples) {
    for document in &mut examples.documents {
        document.file = Some(parse(document.relative, &document.text));
    }
}

#[when("the example configuration is applied to broker defaults")]
fn apply_examples(examples: &mut OperatorExamples) {
    for document in &mut examples.documents {
        let mut config = BrokerConfig::default();
        document
            .file
            .take()
            .expect("a parsed example")
            .apply_to(&mut config)
            .unwrap_or_else(|error| panic!("{} does not apply: {error}", document.relative));
        document.applied = Some(config);
    }
}

#[when("every disabled setting is enabled and parsed as FileConfig")]
fn enable_examples(examples: &mut OperatorExamples) {
    for document in &mut examples.documents {
        document.enabled = enable_disabled_settings(&document.text);
        document.enabled_file = Some(parse(document.relative, &document.enabled));
    }
}

#[when("every disabled setting is enabled and parsed as TOML")]
fn enable_toml(examples: &mut OperatorExamples) {
    for document in &mut examples.documents {
        document.enabled = enable_disabled_settings(&document.text);
        document.table = Some(toml::from_str(&document.enabled).unwrap_or_else(|error| {
            panic!("{} does not parse as TOML: {error}", document.relative)
        }));
    }
}

#[then("every example configures at least one listener")]
fn configured_listeners(examples: &OperatorExamples) {
    for document in &examples.documents {
        check!(
            !document
                .applied
                .as_ref()
                .expect("an applied example")
                .listeners
                .is_empty(),
            "{} should configure at least one listener",
            document.relative
        );
    }
}

#[then("every TLS inter-broker listener has a non-default peer server name")]
fn peer_server_names(examples: &OperatorExamples) {
    let default_name = BrokerConfig::default().inter_broker_server_name;
    for document in &examples.documents {
        let config = document.applied.as_ref().expect("an applied example");
        if config.inter_broker_listener_protocol().requires_tls() {
            check!(
                config.inter_broker_server_name != default_name,
                "{} dials peers over TLS but leaves inter_broker_server_name at {default_name:?}",
                document.relative
            );
        }
    }
}

#[then("each example contains disabled settings and demonstrates operator keys")]
fn operator_keys(examples: &OperatorExamples) {
    for document in &examples.documents {
        assert!(
            document.enabled != document.text,
            "{} should carry at least one disabled setting",
            document.relative
        );
        check!(
            !document
                .enabled_file
                .as_ref()
                .expect("a parsed enabled example")
                .operator_keys
                .is_empty(),
            "{} should show an [[operator_keys]] entry",
            document.relative
        );
    }
}

#[then("the single-node example covers local listeners and recovery features")]
fn local_surface(examples: &OperatorExamples) {
    let file = examples.documents[0]
        .file
        .as_ref()
        .expect("a parsed single-node example");
    check!(file.broker_id == Some(1));
    check!(file.controller_quorum_voters.is_empty());
    check!(file.listeners.len() == 2);
    check!(file.listeners[1].tls_config.is_some());
    check!(file.listeners[1].sasl_config.is_some());
    check!(
        file.remote_storage
            .as_ref()
            .and_then(|storage| storage.storage_dir.as_deref())
            == Some("/var/lib/krabka/tiered")
    );
    check!(file.audit.is_some());
    check!(file.freeze.is_some());
    check!(file.break_glass.is_some());
    check!(file.delegation_token.is_some());
}

#[then("the three-node example covers quorum security and remote storage")]
fn cluster_surface(examples: &OperatorExamples) {
    let document = &examples.documents[0];
    let file = document.file.as_ref().expect("a parsed three-node example");
    check!(file.controller_quorum_voters.len() == 3);
    check!(file.bootstrap_servers.len() == 3);
    check!(file.tls_config.is_some());
    check!(file.stretch.is_some());
    check!(
        file.inter_broker_principal_node_ids
            .as_ref()
            .is_some_and(|bindings| bindings.len() == 3)
    );
    check!(
        file.remote_storage
            .as_ref()
            .is_some_and(|storage| storage.s3.is_some())
    );
    check!(
        file.authorization
            .as_ref()
            .is_some_and(|authorization| authorization.opa.is_some())
    );
    check!(file.schema_registry.is_some());
    let runtime = file.runtime.as_ref().expect("[runtime] present");
    check!(runtime.diskless_wal_flush_interval.is_some());
    check!(runtime.diskless_wal_local_replica_count.is_some());
    let enabled = document
        .enabled_file
        .as_ref()
        .expect("an enabled three-node example");
    check!(enabled.oauthbearer.is_some());
    check!(enabled.gssapi.is_some());
    check!(enabled.inter_broker_credentials.is_some());
    check!(enabled.operator_keys.len() == 2);
    check!(
        enabled
            .remote_storage
            .as_ref()
            .is_some_and(|storage| storage.worm.is_some())
    );
}

#[then("all enabled example keys are known by the configuration schema")]
fn documented_keys(examples: &OperatorExamples) {
    let schema = examples
        .schema
        .as_ref()
        .expect("the checked-in configuration schema");
    for document in &examples.documents {
        let table = document.table.as_ref().expect("a TOML example");
        assert!(!table.is_empty(), "{} sets no keys", document.relative);
        check_table_keys(schema, schema, table, document.relative);
    }
}
#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "examples with a tls inter broker listener name its server"
)]
#[tokio::test]
async fn examples_with_a_tls_inter_broker_listener_name_its_server(examples: OperatorExamples) {}

#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "examples parse and apply as written"
)]
#[tokio::test]
async fn examples_parse_and_apply_as_written(examples: OperatorExamples) {}

#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "examples parse with every disabled setting enabled"
)]
fn examples_parse_with_every_disabled_setting_enabled(examples: OperatorExamples) {}

#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "single node example covers the local surface"
)]
fn single_node_example_covers_the_local_surface(examples: OperatorExamples) {}

#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "three node example covers the cluster surface"
)]
fn three_node_example_covers_the_cluster_surface(examples: OperatorExamples) {}

#[scenario(
    path = "tests/features/migrated/tests/example_broker_toml.feature",
    name = "every example key is in the schema"
)]
fn every_example_key_is_in_the_schema(examples: OperatorExamples) {}
