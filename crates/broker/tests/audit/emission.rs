//! The audit topic itself, and the records that reach it on startup and on an
//! admin operation.
//!
//! The topic has to exist before anything can be audited, the broker's own
//! start has to arrive on it, and every admin mutation has to arrive as an
//! `AdminOperation` record that names the resource it changed. Each case
//! drives the request over a plain client and then reads the record back off
//! the topic, because fetch visibility can lag the durable write.

use {
    crate::support::{
        self, OperatorKeysSetup,
        discovery::named_topic_metadata,
        offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        produce::{SinglePartitionProduceSetup, send_batch},
        records::{batch_from_records, value_record},
        sasl::{ScramAlgorithm, scram_upsertion},
        topics::{creatable_topic, create_topic_request},
    },
    krabka_broker::coordinator::AUDIT_TOPIC,
    krabka_protocol::{
        owned::{
            alter_client_quotas_request::{
                AlterClientQuotasRequest, EntityData, EntryData, OpData as QuotaOp,
            },
            alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            incremental_alter_configs_request::{
                AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
                IncrementalAlterConfigsRequest,
            },
            metadata_request::MetadataRequest,
        },
        primitives::uuid::Uuid,
        records::RecordBatch,
    },
};

/// The KIP-133 `resource_type` discriminant for a topic.
const RESOURCE_TYPE_TOPIC: i8 = 2;
/// The `IncrementalAlterConfigs` `config_operation` for SET.
const OP_SET: i8 = 0;

/// Append one record to partition 0, so that a later `DeleteRecords` has
/// something below the high watermark to delete.
async fn produce_one(p: &support::InProcess, topic: &str, topic_id: Uuid) {
    let batch = RecordBatch {
        last_offset_delta: 0,
        ..batch_from_records(vec![value_record(
            0,
            Some(bytes::Bytes::from_static(b"audited-record")),
        )])
    };
    let resp = send_batch(&p.client, batch, SinglePartitionProduceSetup {
        topic: (topic).into(),
        topic_id,
        ..Default::default()
    })
    .await;
    assert2::check!(resp.responses[0].partition_responses[0].error_code == 0);
}

/// The audit records that name one admin operation.
fn records_for(records: &[serde_json::Value], operation: &str) -> usize {
    records
        .iter()
        .filter(|j| j["class_uid"] == 6003 && j["api"]["operation"] == operation)
        .count()
}

use {
    krabka_broker::BrokerHandle,
    krabka_client_core::{Client, ClientError},
    krabka_protocol::owned::metadata_response::MetadataResponse,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::DataTableRow,
    serde_json::Value,
    tempfile::TempDir,
};
struct AdminMutationOutcome {
    operation: &'static str,
    error: i16,
}
struct AuditEmission {
    plain: Option<support::InProcess>,
    topic: String,
    user: String,
    metadata: Option<MetadataResponse>,
    mutations: Vec<AdminMutationOutcome>,
    records: Option<Vec<Value>>,
    sasl_broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    reader: Option<Client>,
    bad_login: Option<Result<(), ClientError>>,
}
#[rstest::fixture]
fn emission() -> AuditEmission {
    // Start this scenario with fresh inputs and observations.

    AuditEmission {
        plain: None,
        topic: String::new(),
        user: String::new(),
        metadata: None,
        mutations: Vec::new(),
        records: None,
        sasl_broker: None,
        directory: None,
        bootstrap: String::new(),
        reader: None,
        bad_login: None,
    }
}
#[rstest_bdd_macros::given("a fresh broker with its audit partition initialized")]
async fn audit_emission_broker(emission: &mut AuditEmission) {
    let broker = support::start().await;
    broker
        .broker
        .wait_until_partition_present(AUDIT_TOPIC, 0)
        .await;
    emission.plain = Some(broker);
}
#[rstest_bdd_macros::when("a client requests Metadata for the audit topic")]
async fn audit_topic_metadata(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let resp = p
        .client
        .send(named_topic_metadata(AUDIT_TOPIC))
        .await
        .expect("MetadataRequest failed");

    emission.metadata = Some(resp);
}
#[rstest_bdd_macros::then("the audit topic is listed without error and has at least one partition")]
fn audit_topic_initialized(emission: &AuditEmission) {
    let resp = emission
        .metadata
        .as_ref()
        .expect("audit Metadata requested");
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__krabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__krabka_audit has no partitions"
    );
}
#[rstest_bdd_macros::when("a client fetches the durable startup audit record")]
async fn read_broker_start_event(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    // Wait for the BrokerStarted event to be durably written to the audit topic
    // (the sink increments `audit_events` on each successful produce).
    p.broker
        .wait_for_metrics("audit event written", |m| m.audit_events.get() >= 1)
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    let records = support::wait_for_audit_record(&p.client, "BrokerStarted", |j| {
        j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted"
    })
    .await;

    emission.records = Some(records);
}
#[rstest_bdd_macros::then("the audit topic contains a BrokerStarted activity of class 6002")]
fn broker_start_event_audited(emission: &AuditEmission) {
    assert2::check!(
        emission
            .records
            .as_ref()
            .expect("audit records fetched")
            .iter()
            .any(|row| row["class_uid"] == 6002 && row["activity_name"] == "BrokerStarted")
    );
}
#[rstest_bdd_macros::given("admin mutation inputs name topic {topic} and user {user}")]
fn audit_mutation_inputs(emission: &mut AuditEmission, topic: String, user: String) {
    emission.topic = topic;
    emission.user = user;
}
#[rstest_bdd_macros::when("the admin creates the audit example topic")]
async fn audited_created(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let created = p
        .client
        .send(create_topic_request(
            creatable_topic().name((topic).into()).call(),
        ))
        .await
        .expect("CreateTopics");
    emission.mutations.push(AdminMutationOutcome {
        operation: "CreateTopics",
        error: created.topics[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin grows the audit example topic to two partitions")]
async fn audited_grown(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let grown = p
        .client
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: topic.into(),
                count: 2,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreatePartitions");
    emission.mutations.push(AdminMutationOutcome {
        operation: "CreatePartitions",
        error: grown.results[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin sets retention to 604800000")]
async fn audited_altered(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let altered = p
        .client
        .send(AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![AlterableConfig {
                    name: "retention.ms".into(),
                    value: Some("604800000".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterConfigs");
    emission.mutations.push(AdminMutationOutcome {
        operation: "AlterConfigs",
        error: altered.responses[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin incrementally sets maximum message bytes to 1048576")]
async fn audited_incremental(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let incremental = p
        .client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![IncrementalConfig {
                    name: "max.message.bytes".into(),
                    config_operation: OP_SET,
                    value: Some("1048576".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    emission.mutations.push(AdminMutationOutcome {
        operation: "IncrementalAlterConfigs",
        error: incremental.responses[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin replaces all overrides with cleanup policy delete")]
async fn audited_replaced(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let replaced = p
        .client
        .send(AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![AlterableConfig {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterConfigs (replacement)");
    emission.mutations.push(AdminMutationOutcome {
        operation: "AlterConfigs replacement",
        error: replaced.responses[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin sets the audit user producer byte quota to {rate}")]
async fn audited_quotas(emission: &mut AuditEmission, rate: f64) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let user = emission.user.as_str();
    let quotas = p
        .client
        .send(AlterClientQuotasRequest {
            entries: vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some(user.into()),
                    ..Default::default()
                }],
                ops: vec![QuotaOp {
                    key: "producer_byte_rate".into(),
                    value: rate,
                    remove: false,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterClientQuotas");
    emission.mutations.push(AdminMutationOutcome {
        operation: "AlterClientQuotas",
        error: quotas.entries[0].error_code,
    });
}
#[rstest_bdd_macros::when(
    "the admin provisions audit user SCRAM SHA 256 with salt {salt} and salted password {password}"
)]
async fn audited_scram(emission: &mut AuditEmission, salt: String, password: String) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let user = emission.user.as_str();
    let scram = p
        .client
        .send(AlterUserScramCredentialsRequest {
            upsertions: vec![
                scram_upsertion()
                    .name((user).into())
                    .mechanism(ScramAlgorithm::Sha256)
                    .salt(bytes::Bytes::from(salt))
                    .salted_password(bytes::Bytes::from(password))
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("AlterUserScramCredentials");
    emission.mutations.push(AdminMutationOutcome {
        operation: "AlterUserScramCredentials",
        error: scram.results[0].error_code,
    });
}
#[rstest_bdd_macros::when("the admin deletes the audit example topic")]
async fn audited_deleted(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let deleted = p
        .client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    emission.mutations.push(AdminMutationOutcome {
        operation: "DeleteTopics",
        error: deleted.responses[0].error_code,
    });
}
#[rstest_bdd_macros::then("the {operation} admin mutation succeeds")]
fn audited_mutation_succeeded(emission: &AuditEmission, operation: String) {
    let result = emission
        .mutations
        .iter()
        .rev()
        .find(|result| result.operation == operation)
        .expect("the named mutation was attempted");
    assert2::check!(
        result.error == 0,
        "{} failed with {}",
        result.operation,
        result.error
    );
}
#[rstest_bdd_macros::given("the audit example topic has two records eligible for trimming")]
async fn audit_trim_input(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let topic = emission.topic.as_str();
    let topic_id = support::topic_id_for(&p.client, topic).await;
    for _ in 0..2 {
        produce_one(p, topic, topic_id).await;
    }
}
#[rstest_bdd_macros::when("the admin trims the topic before offset 2 and retries the same trim")]
async fn audit_retried_trim(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let request = delete_records_request(
        vec![delete_records_topic(&emission.topic, vec![
            delete_records_partition(0, 2),
        ])],
        5_000,
    );
    for operation in ["DeleteRecords", "DeleteRecords retry"] {
        let response = p.client.send(request.clone()).await.expect(operation);
        emission.mutations.push(AdminMutationOutcome {
            operation,
            error: response.topics[0].partitions[0].error_code,
        });
    }
}
#[derive(DataTableRow)]
struct ExpectedMutationAudit {
    operation: String,
    resource: String,
}
#[rstest_bdd_macros::then(
    "each successful admin mutation has audit evidence naming its changed resource:"
)]
async fn audit_all_mutations(
    emission: &AuditEmission,
    #[datatable] rows: Rows<ExpectedMutationAudit>,
) {
    let p = emission.plain.as_ref().expect("audit broker started");
    for row in rows {
        support::wait_for_audit_record(&p.client, &row.operation, |record| {
            record["class_uid"] == 6003
                && record["api"]["operation"] == row.operation
                && record["status_id"] == 1
                && record["resources"][0]["name"] == row.resource
        })
        .await;
    }
}
#[rstest_bdd_macros::then(
    "the replacement audit row names cleanup policy and both removed override keys"
)]
async fn audit_replacement_keys(emission: &AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    let dropped = ["retention.ms", "max.message.bytes"];
    // A replacement deletes every stored key it omits, so the record for it
    // names the keys that disappeared beside the one it set.
    support::wait_for_audit_record(&p.client, "AlterConfigs replacement", |j| {
        j["api"]["operation"] == "AlterConfigs"
            && j["resources"].as_array().is_some_and(|resources| {
                let named = |key: &str| resources.iter().any(|r| r["name"] == key);
                named("cleanup.policy") && dropped.iter().copied().all(named)
            })
    })
    .await;
}
#[rstest_bdd_macros::when("the client consumes the full audit topic after all mutations")]
async fn read_all_mutation_audits(emission: &mut AuditEmission) {
    let p = emission.plain.as_ref().expect("audit broker started");
    emission.records = Some(support::consume_audit_records(&p.client).await);
}
#[rstest_bdd_macros::then(
    "the trim is audited once and secrets are redacted while changed config keys remain visible"
)]
fn audit_redaction_and_retry(emission: &AuditEmission) {
    let records = emission
        .records
        .as_ref()
        .expect("full audit topic consumed");
    // The retried trim deleted nothing, and an audit record claiming it did
    // would be false evidence of a second deletion.
    assert2::check!(
        records_for(records, "DeleteRecords") == 1,
        "an idempotent DeleteRecords retry must audit nothing"
    );
    let dump = serde_json::to_string(records).expect("audit records serialize");
    for secret in [
        "audited-salt",
        "audited-salted-password-32-byte",
        "604800000",
    ] {
        assert2::check!(!dump.contains(secret), "audit topic leaked {secret}");
    }

    // The changed config *keys* are named, because that is what says a
    // retention or a message-size limit moved.
    assert2::check!(dump.contains("retention.ms"));
    assert2::check!(dump.contains("max.message.bytes"));
}
#[rstest_bdd_macros::given("a SASL audit broker accepts alice password alice-secret")]
async fn audit_login_broker(emission: &mut AuditEmission) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl()
        .dir(&dir.path().join("data"))
        .trust(OperatorKeysSetup {
            keys: &[],
            approvers: &[],
        })
        .users(&[("alice", "alice-secret")])
        .call()
        .await;
    broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    emission.directory = Some(dir);
    emission.sasl_broker = Some(broker);
    emission.bootstrap = bootstrap;
}
#[rstest_bdd_macros::when("alice authenticates successfully to read the audit topic")]
async fn audit_login_success(emission: &mut AuditEmission) {
    emission.reader =
        Some(support::sasl_client(&emission.bootstrap, "alice", "alice-secret").await);
}
#[rstest_bdd_macros::when("another alice connection uses the wrong password and requests Metadata")]
async fn audit_login_failure(emission: &mut AuditEmission) {
    let connection = Client::builder()
        .bootstrap(&emission.bootstrap)
        .client_id("krabka-broker-test")
        .security(support::sasl_plain_security("alice", "not-the-password"))
        .build()
        .await;
    emission.bad_login = Some(match connection {
        Ok(client) => client.send(MetadataRequest::default()).await.map(|_| ()),
        Err(error) => Err(error),
    });
}
#[rstest_bdd_macros::then("the wrong password connection serves no request")]
fn audit_bad_login_rejected(emission: &AuditEmission) {
    assert2::check!(
        emission
            .bad_login
            .as_ref()
            .expect("wrong-password request attempted")
            .is_err()
    );
}
#[rstest_bdd_macros::then(
    "both successful and refused PLAIN logins name User alice and the loopback peer"
)]
async fn audit_both_login_results(emission: &AuditEmission) {
    let client = emission.reader.as_ref().expect("alice authenticated");
    // Both rows name the same principal in the `User:<name>` form the
    // privileged-action rows use, and carry the peer they came from.
    for (what, status_id) in [("the successful login", 1), ("the refused login", 2)] {
        support::wait_for_audit_record(client, what, |j| {
            j["class_uid"] == 3002
                && j["status_id"] == status_id
                && j["auth_protocol"] == "PLAIN"
                && j["actor"]["user"]["name"] == "User:alice"
                && j["src_endpoint"]["ip"] == "127.0.0.1"
        })
        .await;
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/emission.feature",
    name = "audit topic exists after startup"
)]
#[tokio::test]
async fn audit_topic_exists_after_startup(emission: AuditEmission) {
    emission
        .plain
        .expect("audit broker started")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/emission.feature",
    name = "broker started event is written to audit topic"
)]
#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic(emission: AuditEmission) {
    emission
        .plain
        .expect("audit broker started")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/emission.feature",
    name = "every admin mutation is audited"
)]
#[tokio::test]
async fn every_admin_mutation_is_audited(emission: AuditEmission) {
    emission
        .plain
        .expect("audit broker started")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/emission.feature",
    name = "sasl plain logins are audited either way"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_logins_are_audited_either_way(emission: AuditEmission) {
    emission
        .sasl_broker
        .expect("SASL broker started")
        .shutdown()
        .await;
}
