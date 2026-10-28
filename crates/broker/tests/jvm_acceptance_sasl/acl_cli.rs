//! Kafka ACL administration and shorthand expansion, checked through real JVM clients.
use {
    crate::{
        acl_output::{AclBinding, parse_acls},
        jvm_acceptance::{
            ADMIN, ADMIN_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, broker0_advertised,
            docker_run_kafka_tool_with_image_and_mount, nc_check_connectivity,
            plain_client_properties, start_sasl_plaintext_broker_with_super_user,
            write_plain_props,
        },
        oracle::{CliRun, Oracle, SecuredSide, Side, ToolFile, secured_sides},
    },
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    std::{
        collections::{BTreeMap, BTreeSet},
        process::Output,
    },
    tempfile::TempDir,
    tokio::task,
};
const CONTAINER_PROPS: &str = "/tmp/client.properties";
fn acls(side: &Side<'_>, props: Option<&str>, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    let mut files = Vec::new();
    if let Some(props) = props {
        full.extend_from_slice(&["--command-config", CONTAINER_PROPS]);
        files.push(ToolFile::new(CONTAINER_PROPS, props));
    }
    side.run_with_files("kafka-acls", &full, &files, None)
}

struct Provision {
    broker: Option<(BrokerHandle, TempDir)>,
    props: Option<ClientPropsFile>,
    output: Option<Output>,
    listed: Option<Output>,
}
#[rstest::fixture]
fn provision() -> Provision {
    // Start this scenario with fresh inputs and observations.

    Provision {
        broker: None,
        props: None,
        output: None,
        listed: None,
    }
}
#[rstest_bdd_macros::given(
    "a PLAIN SASL broker authenticates admin as superuser for Kafka ACL provisioning"
)]
async fn provision_broker(provision: &mut Provision) {
    provision.broker =
        Some(start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await);
    nc_check_connectivity();
    provision.props = Some(write_plain_props(ADMIN, ADMIN_PASS));
}
#[rstest_bdd_macros::when("the Kafka 3.5 ACL tool applies these arguments and lists topic foo")]
fn provision_acl(provision: &mut Provision, docstring: String) {
    let mount = provision.props.as_ref().expect("properties").mount_str();
    let mut args = vec![
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
    ];
    args.extend(docstring.split_whitespace());
    provision.output = Some(docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &args,
    ));
    provision.listed = Some(docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--list",
            "--topic",
            "foo",
        ],
    ));
}
#[rstest_bdd_macros::then("the listing contains these ACL fields")]
fn provision_fields(provision: &Provision, datatable: Vec<Vec<String>>) {
    let listed = String::from_utf8_lossy(&provision.listed.as_ref().expect("listed").stdout);
    for row in datatable.into_iter().skip(1) {
        let [field]: [String; 1] = row.try_into().expect("field");
        if matches!(field.as_str(), "READ" | "ALLOW") {
            check!(listed.to_ascii_uppercase().contains(&field), "{listed}");
        } else {
            check!(listed.contains(&field), "{listed}");
        }
    }
}
#[rstest_bdd_macros::then("the listing no longer contains User:alice")]
fn removed_binding(provision: &Provision) {
    assert!(
        !String::from_utf8_lossy(&provision.listed.as_ref().expect("listed").stdout)
            .contains("User:alice")
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_cli.feature",
    name = "jvm kafka acls provision via cli"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_kafka_acls_provision_via_cli(provision: Provision) {
    provision.broker.take().expect("broker").0.shutdown().await;
    drop(provision);
}

struct ExpandedAcl {
    principal: String,
    added: CliRun,
    listed: CliRun,
}
struct Shorthands {
    oracle: Option<Oracle>,
    broker: Option<(BrokerHandle, TempDir)>,
    expansions: Vec<ExpandedAcl>,
    removals: Vec<(CliRun, CliRun)>,
}
#[rstest::fixture]
fn shorthands() -> Shorthands {
    // Start this scenario with fresh inputs and observations.

    Shorthands {
        oracle: None,
        broker: None,
        expansions: Vec::new(),
        removals: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka authorizes anonymous administration and Krabka authorizes the PLAIN admin superuser"
)]
async fn comparable_acl_brokers(shorthands: &mut Shorthands) {
    shorthands.oracle = Some(task::spawn_blocking(|| Oracle::start_with_env("acls", &["KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer", "KAFKA_SUPER_USERS=User:ANONYMOUS", "KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false"])).await.expect("oracle"));
    shorthands.broker =
        Some(start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await);
    nc_check_connectivity();
}
#[rstest_bdd_macros::when(
    "both Kafka ACL clients add these shorthand bindings and list only the named principal"
)]
fn expand_shorthands(shorthands: &mut Shorthands, datatable: Vec<Vec<String>>) {
    let oracle_side = Side::Oracle(shorthands.oracle.as_ref().expect("oracle"));
    let krabka_side = Side::Krabka {
        bootstrap: broker0_advertised(),
    };
    let admin_props = plain_client_properties(ADMIN, ADMIN_PASS);
    for row in datatable.into_iter().skip(1) {
        let [principal, arguments]: [String; 2] = row.try_into().expect("principal arguments");
        let args: Vec<&str> = arguments.split_whitespace().collect();
        for SecuredSide { side, props } in secured_sides(&oracle_side, &krabka_side, &admin_props) {
            let added = acls(side, props, &args);
            let listed = acls(side, props, &["--list", "--principal", &principal]);
            shorthands.expansions.push(ExpandedAcl {
                principal: principal.clone(),
                added,
                listed,
            });
        }
    }
}
#[rstest_bdd_macros::then(
    "both listings contain these required bindings, isolate each principal, and match Kafka exactly"
)]
fn expansion_parity(shorthands: &Shorthands, datatable: Vec<Vec<String>>) {
    let mut expected: BTreeMap<String, BTreeSet<AclBinding>> = BTreeMap::new();
    for row in datatable.into_iter().skip(1) {
        let [
            principal,
            host,
            resource_type,
            resource_name,
            operation,
            permission,
        ]: [String; 6] = row
            .try_into()
            .expect("principal host resource type name operation permission");
        expected
            .entry(principal.clone())
            .or_default()
            .insert(AclBinding {
                resource_type,
                resource_name,
                pattern_type: "LITERAL".to_owned(),
                principal,
                host,
                operation,
                permission,
            });
    }
    assert!(!shorthands.expansions.is_empty());
    for pair in shorthands.expansions.as_chunks::<2>().0 {
        let mut bindings = Vec::new();
        for expansion in pair {
            assert!(expansion.added.succeeded(), "{}", expansion.added.text());
            assert!(expansion.listed.succeeded(), "{}", expansion.listed.text());
            let actual = parse_acls(&expansion.listed.stdout);
            assert!(
                expected
                    .get(&expansion.principal)
                    .expect("required bindings")
                    .is_subset(&actual)
            );
            assert!(
                actual
                    .iter()
                    .all(|binding| binding.principal == expansion.principal)
            );
            bindings.push(actual);
        }
        assert!(bindings[0] == bindings[1]);
    }
}
#[rstest_bdd_macros::when(
    "both clients remove these shorthand arguments and list principal {principal}"
)]
fn remove_shorthand(shorthands: &mut Shorthands, principal: String, docstring: String) {
    let oracle_side = Side::Oracle(shorthands.oracle.as_ref().expect("oracle"));
    let krabka_side = Side::Krabka {
        bootstrap: broker0_advertised(),
    };
    let props = plain_client_properties(ADMIN, ADMIN_PASS);
    let args: Vec<&str> = docstring.split_whitespace().collect();
    for SecuredSide { side, props } in secured_sides(&oracle_side, &krabka_side, &props) {
        shorthands.removals.push((
            acls(side, props, &args),
            acls(side, props, &["--list", "--principal", &principal]),
        ));
    }
}
#[rstest_bdd_macros::then("both removals and listings succeed and no consumer bindings remain")]
fn removed_shorthand(shorthands: &Shorthands) {
    assert!(shorthands.removals.len() == 2);
    for (removed, listed) in &shorthands.removals {
        assert!(removed.succeeded(), "{}", removed.text());
        assert!(listed.succeeded(), "{}", listed.text());
        assert!(parse_acls(&listed.stdout).is_empty());
    }
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_cli.feature",
    name = "acl shorthands expand as apache kafka expands them"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acl_shorthands_expand_as_apache_kafka_expands_them(shorthands: Shorthands) {
    shorthands.broker.take().expect("broker").0.shutdown().await;
    drop(shorthands);
}
