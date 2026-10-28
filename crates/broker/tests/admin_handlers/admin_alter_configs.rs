//! `AlterConfigs` (`api_key` 33): the round-trip that pushes a topic override
//! into the partition's log, the rejection of an unknown config key, and the
//! `min.insync.replicas` pre-flight that gates an `acks=-1` produce but leaves
//! `acks=1` alone.

use {
    crate::{
        RESOURCE_TYPE_TOPIC,
        admin_harness::{AdminCluster, admin, create_topic_helper},
        support::{
            configs::{legacy_config, legacy_request, legacy_resource},
            discovery::named_topic_metadata,
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{batch_from_records, value_record},
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_protocol::{
        owned::{alter_configs_response::AlterConfigsResponse, produce_response::ProduceResponse},
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::{Duration, Instant},
    tokio::task,
};

struct TopicSettings {
    name: String,
    altered: Option<AlterConfigsResponse>,
    topic_id: Option<WireUuid>,
    batch: Option<RecordBatch>,
    produced: Option<ProduceResponse>,
    acknowledgements: String,
}

#[fixture]
fn settings() -> TopicSettings {
    // Start this scenario with fresh inputs and observations.

    TopicSettings {
        name: String::new(),
        altered: None,
        topic_id: None,
        batch: None,
        produced: None,
        acknowledgements: String::new(),
    }
}

#[given("topic {name} exists with one local replica")]
async fn local_topic(admin: &AdminCluster, settings: &mut TopicSettings, name: String) {
    create_topic_helper(admin.client(), &name, 1).await;
    admin.broker().wait_until_partition_present(&name, 0).await;
    settings.name = name;
}

#[when("AlterConfigs sets {key} to {value}")]
async fn alter_topic_config(
    admin: &AdminCluster,
    settings: &mut TopicSettings,
    key: String,
    value: String,
) {
    let request = legacy_request(
        vec![legacy_resource(
            RESOURCE_TYPE_TOPIC,
            settings.name.clone(),
            vec![legacy_config(key, Some(value))],
        )],
        false,
    );
    settings.altered = Some(admin.client().send(request).await.expect("alter_configs"));
}

#[then("AlterConfigs accepts the topic configuration")]
fn accepted_alter(settings: &TopicSettings) {
    let response = settings
        .altered
        .as_ref()
        .expect("AlterConfigs was submitted");
    assert!(
        response.responses[0].error_code == 0,
        "alter_configs response: {:?}",
        response.responses[0].error_message
    );
}

#[then("the local partition adopts a 60000 millisecond retention within 10 seconds")]
async fn local_retention(admin: &AdminCluster, settings: &TopicSettings) {
    let broker = admin.broker();
    let deadline = Instant::now() + Duration::from_secs(10);
    let want = Duration::from_mins(1);
    let last = loop {
        let cur = broker
            .partition_retention_ms_for_test(&settings.name, 0)
            .and_then(|inner| inner);
        if cur == Some(want) {
            break cur;
        }
        if Instant::now() > deadline {
            break cur;
        }
        task::yield_now().await;
    };
    assert!(
        last == Some(want),
        "retention_ms did not converge within 10 s after AlterConfigs"
    );
}

#[then("AlterConfigs rejects unknown key {key} with INVALID_CONFIG and names it in the error")]
fn rejected_key(settings: &TopicSettings, key: String) {
    let response = settings
        .altered
        .as_ref()
        .expect("AlterConfigs was submitted");
    let result = &response.responses[0];
    assert!(
        result.error_code == 40,
        "expected INVALID_CONFIG(40), got {}",
        result.error_code
    );
    assert!(
        result.error_message.as_deref().unwrap_or("").contains(&key),
        "expected error_message to mention {key:?}, got {:?}",
        result.error_message
    );
}

#[given("min.insync.replicas is 2 on that single-replica topic")]
async fn clamped_minimum_isr(admin: &AdminCluster, settings: &mut TopicSettings) {
    let metadata = admin
        .client()
        .send(named_topic_metadata(&settings.name))
        .await
        .expect("Metadata for topic_id");
    settings.topic_id = Some(
        metadata
            .topics
            .iter()
            .find(|topic| topic.name.as_deref() == Some(settings.name.as_str()))
            .expect("topic in Metadata response")
            .topic_id,
    );
    alter_topic_config(admin, settings, "min.insync.replicas".into(), "2".into()).await;
    accepted_alter(settings);
    settings.batch = Some(RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 0,
        ..batch_from_records(vec![value_record(0, Some(Bytes::from_static(b"x")))])
    });
}

#[when("one record is produced with acknowledgement mode {mode}")]
async fn produce_under_minimum_isr(
    admin: &AdminCluster,
    settings: &mut TopicSettings,
    mode: String,
) {
    let setup = match mode.as_str() {
        "all" => SinglePartitionProduceSetup::replicated(),
        "leader" => SinglePartitionProduceSetup::default(),
        other => panic!("unknown acknowledgement mode {other}"),
    };
    let batch = settings.batch.as_ref().expect("a prepared record").clone();
    settings.produced = Some(
        admin
            .client()
            .send(batch_request(batch, SinglePartitionProduceSetup {
                topic: settings.name.clone(),
                topic_id: settings.topic_id.expect("the topic id was resolved"),
                ..setup
            }))
            .await
            .expect("Produce with min.insync.replicas"),
    );
    settings.acknowledgements = mode;
}

#[then("the produce request succeeds despite the configured minimum exceeding the replica count")]
fn clamped_produce(settings: &TopicSettings) {
    let result = &settings
        .produced
        .as_ref()
        .expect("a produce request")
        .responses[0]
        .partition_responses[0];
    assert!(
        result.error_code == 0,
        "{} acknowledgements must accept ISR={{1}} with min(2,1)=1; got {}",
        settings.acknowledgements,
        result.error_code
    );
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_alter_configs.feature",
    name = "alter configs round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_round_trip(admin: AdminCluster, settings: TopicSettings) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_alter_configs.feature",
    name = "alter configs rejects unknown key"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_rejects_unknown_key(admin: AdminCluster, settings: TopicSettings) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_alter_configs.feature",
    name = "min insync replicas is clamped to the replica count for acks all"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_insync_replicas_is_clamped_to_the_replica_count_for_acks_all(
    admin: AdminCluster,
    settings: TopicSettings,
) {
    admin.shutdown().await;
}
