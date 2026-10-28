//! KIP-211 `offsets.retention.minutes`, end to end over the wire.
//!
//! Two cases read the retention knobs back through `DescribeConfigs`, the way
//! `kafka-configs --entity-type brokers --describe` does: an untuned broker
//! reports both at `DEFAULT_CONFIG`, and a broker whose configuration names a
//! key reports that key at `STATIC_BROKER_CONFIG`. The third lets the broker's
//! own background sweep run and watches a dead group disappear from
//! `ListGroups`.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::connect_owned,
        commit_partition_zero,
        configs::describe_resource,
        create_topic_for_commits, heartbeat_config,
        offsets::{offset_delete_partition, offset_delete_request, offset_delete_topic},
        standalone_broker,
    },
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerHandle, codes},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
        list_groups_request::ListGroupsRequest,
        list_groups_response::ListGroupsResponse,
        offset_delete_response::OffsetDeleteResponse,
    },
    krabka_units::{millis, minutes},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::{TempDir, tempdir},
    tokio::time,
};

/// `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `ConfigEntry.ConfigSource.DEFAULT_CONFIG`. Verified against
/// `apache/kafka:4.3.1`: `kafka-configs --describe --all` reports both keys
/// with `DEFAULT_CONFIG` on a broker whose properties do not set them.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG`. Verified against
/// `apache/kafka:4.3.1`: a broker whose properties carry
/// `offsets.retention.minutes=10080` — Kafka's own default — reports that key
/// with `STATIC_BROKER_CONFIG` at the head of its synonym chain, because the
/// source says where the value came from, not whether it differs from the
/// default.
const CONFIG_SOURCE_STATIC_BROKER: i8 = 4;
/// `DescribeConfigsResponse.ConfigType`, which mirrors `ConfigDef.Type`.
/// `GroupCoordinatorConfig` declares `offsets.retention.minutes` an `INT` and
/// `offsets.retention.check.interval.ms` a `LONG`.
const CONFIG_TYPE_INT: i8 = 3;
const CONFIG_TYPE_LONG: i8 = 5;
const OFFSETS_RETENTION_MINUTES: &str = "offsets.retention.minutes";
const OFFSETS_RETENTION_CHECK_INTERVAL_MS: &str = "offsets.retention.check.interval.ms";

#[bon::builder]
fn expected_config(
    name: &str,
    value: &str,
    config_source: i8,
    config_type: i8,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.into(),
        value: Some(value.into()),
        read_only: true,
        config_source,
        config_type,
        documentation: None,
        ..Default::default()
    }
}

async fn client_for(broker: &krabka_broker::BrokerHandle) -> Arc<Client> {
    Arc::new(
        connect_owned(
            broker.listen_addr().to_string().as_str(),
            "retention-test",
            "client",
        )
        .await,
    )
}

struct RetentionDescription {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Arc<Client>>,
    response: Option<DescribeConfigsResponse>,
}

#[fixture]
fn retention() -> RetentionDescription {
    // Start this scenario with fresh inputs and observations.

    RetentionDescription {
        directory: None,
        broker: None,
        client: None,
        response: None,
    }
}

#[given("a broker has no explicit offset retention settings")]
async fn default_retention(retention: &mut RetentionDescription) {
    let (directory, broker) = standalone_broker().await;
    retention.client = Some(client_for(&broker).await);
    retention.broker = Some(broker);
    retention.directory = Some(directory);
}

#[given("a broker explicitly sets offset retention to Kafka's default 10080 minutes")]
async fn explicit_retention(retention: &mut RetentionDescription) {
    let directory = tempdir().unwrap();
    let mut config = heartbeat_config(directory.path());
    config.offsets_retention_override = Some(minutes(10_080));
    let broker = Broker::start(config).await.unwrap();
    retention.client = Some(client_for(&broker).await);
    retention.broker = Some(broker);
    retention.directory = Some(directory);
}

async fn request_retention(retention: &mut RetentionDescription, keys: Option<Vec<String>>) {
    let client = retention
        .client
        .as_ref()
        .expect("a configured broker was started");
    retention.response = Some(
        client
            .send(DescribeConfigsRequest {
                resources: vec![describe_resource(
                    RESOURCE_TYPE_BROKER,
                    "1".to_owned(),
                    keys,
                )],
                include_synonyms: false,
                include_documentation: false,
                ..Default::default()
            })
            .await
            .expect("DescribeConfigs"),
    );
}

#[when("all broker configurations are described without synonyms or documentation")]
async fn describe_defaults(retention: &mut RetentionDescription) {
    request_retention(retention, None).await;
}

#[when("only the two retention configuration keys are described")]
async fn describe_keys(retention: &mut RetentionDescription) {
    request_retention(
        retention,
        Some(vec![
            OFFSETS_RETENTION_MINUTES.into(),
            OFFSETS_RETENTION_CHECK_INTERVAL_MS.into(),
        ]),
    )
    .await;
}

fn expected_retention(rows: Vec<Vec<String>>) -> Vec<DescribeConfigsResourceResult> {
    rows.into_iter()
        .skip(1)
        .map(|row| {
            let [name, value, source, kind]: [String; 4] =
                row.try_into().expect("name, value, source and type");
            let source = match source.as_str() {
                "default" => CONFIG_SOURCE_DEFAULT,
                "static broker" => CONFIG_SOURCE_STATIC_BROKER,
                _ => panic!("unknown configuration source: {source}"),
            };
            let kind = match kind.as_str() {
                "INT" => CONFIG_TYPE_INT,
                "LONG" => CONFIG_TYPE_LONG,
                _ => panic!("unknown retention configuration type: {kind}"),
            };
            expected_config()
                .name(&name)
                .value(&value)
                .config_source(source)
                .config_type(kind)
                .call()
        })
        .collect()
}

impl RetentionDescription {
    fn successful_result(&self) -> &DescribeConfigsResult {
        let result = &self
            .response
            .as_ref()
            .expect("DescribeConfigs completed")
            .results[0];
        check!(result.error_code == codes::NONE);
        result
    }
}

#[then("the read-only retention entries include")]
fn default_entries(retention: &RetentionDescription, datatable: Vec<Vec<String>>) {
    let result = retention.successful_result();
    for expected in expected_retention(datatable) {
        let entry = result
            .configs
            .iter()
            .find(|entry| entry.name == expected.name);
        check!(entry == Some(&expected));
    }
}

#[then("the read-only retention entries are exactly")]
fn exact_entries(retention: &RetentionDescription, datatable: Vec<Vec<String>>) {
    let result = retention.successful_result();
    check!(result.configs == expected_retention(datatable));
}

struct DeadGroupSweep {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Arc<Client>>,
    initial: Option<ListGroupsResponse>,
    deleted: Option<OffsetDeleteResponse>,
    final_listing: Option<ListGroupsResponse>,
}

#[fixture]
fn sweep() -> DeadGroupSweep {
    // Start this scenario with fresh inputs and observations.

    DeadGroupSweep {
        directory: None,
        broker: None,
        client: None,
        initial: None,
        deleted: None,
        final_listing: None,
    }
}

#[given(
    "memberless group leaked-by-ci has offset five on orders and the broker sweeps every 25 milliseconds"
)]
async fn group_with_offset(sweep: &mut DeadGroupSweep) {
    let directory = tempdir().unwrap();
    let mut config = heartbeat_config(directory.path());
    config.offsets_retention_check_interval_override = Some(millis(25));
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let client = client_for(&broker).await;
    let topic_id = create_topic_for_commits(&client, "orders").await;
    commit_partition_zero(&client, "leaked-by-ci", ("orders", topic_id), 5).await;
    sweep.client = Some(client);
    sweep.broker = Some(broker);
    sweep.directory = Some(directory);
}

#[when("the memberless groups are listed")]
async fn initial_groups(sweep: &mut DeadGroupSweep) {
    sweep.initial = Some(
        sweep
            .client
            .as_ref()
            .expect("a group was created")
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups"),
    );
}

#[then("leaked-by-ci appears in the successful group listing")]
fn initially_listed(sweep: &DeadGroupSweep) {
    let response = sweep.initial.as_ref().expect("ListGroups completed");
    assert!(response.error_code == codes::NONE);
    assert!(
        response
            .groups
            .iter()
            .any(|group| group.group_id == "leaked-by-ci"),
        "the group starts listed"
    );
}

#[when("the group's last committed offset is deleted")]
async fn delete_last_offset(sweep: &mut DeadGroupSweep) {
    sweep.deleted = Some(
        sweep
            .client
            .as_ref()
            .expect("a group was created")
            .send(offset_delete_request("leaked-by-ci", vec![
                offset_delete_topic("orders", vec![offset_delete_partition(0)]),
            ]))
            .await
            .expect("OffsetDelete"),
    );
}

#[then("the last offset deletion succeeds")]
fn deleted_last_offset(sweep: &DeadGroupSweep) {
    let response = sweep.deleted.as_ref().expect("OffsetDelete completed");
    assert!(response.error_code == codes::NONE);
    assert!(response.topics[0].partitions[0].error_code == codes::NONE);
}

#[when("the broker's own sweeper is observed for up to ten seconds")]
async fn await_sweeper(sweep: &mut DeadGroupSweep) {
    let client = sweep.client.as_ref().expect("a group was created");
    let deadline = Instant::now() + Duration::from_secs(10);
    sweep.final_listing = Some(loop {
        let response = client
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups");
        if !response
            .groups
            .iter()
            .any(|group| group.group_id == "leaked-by-ci")
            || Instant::now() >= deadline
        {
            break response;
        }
        time::sleep(Duration::from_millis(25)).await;
    });
}

#[then("the dead group disappears without a DeleteGroups request")]
fn reaped_group(sweep: &DeadGroupSweep) {
    let response = sweep
        .final_listing
        .as_ref()
        .expect("the sweeper was observed");
    assert!(response.error_code == codes::NONE);
    assert!(
        !response
            .groups
            .iter()
            .any(|group| group.group_id == "leaked-by-ci"),
        "the sweep never reaped the dead group"
    );
}

async fn stop_retention(retention: RetentionDescription) {
    retention
        .broker
        .expect("a configured broker was started")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/offsets_retention.feature",
    name = "Default retention knobs retain Kafka values sources and scalar types"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_retention_knobs(retention: RetentionDescription) {
    stop_retention(retention).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offsets_retention.feature",
    name = "An explicitly configured default still reports its static broker source"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_a_named_knob_as_static(retention: RetentionDescription) {
    stop_retention(retention).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offsets_retention.feature",
    name = "The background sweeper removes a memberless group after its final offset is deleted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_broker_sweep_reaps_a_dead_group_on_its_own(sweep: DeadGroupSweep) {
    sweep.broker.expect("a broker was started").shutdown().await;
}
