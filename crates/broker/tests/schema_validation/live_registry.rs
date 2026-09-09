//! M19: the broker's schema gate against the real first-party registry.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::schema_validation::SchemaValidator;
use krabka_client_core::Client;
use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use krabka_schema_registry::{
    config::{RegistryConfig, RegistryRuntimeConfig, SecurityConfig},
    election::{Election, PrimaryState},
    kafkastore::KafkaStore,
    rest::{self, AppState, forward::ForwardState},
};
use krabka_units::{minutes, secs};
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    harness::{
        INVALID_RECORD, batch_with_value, create_topic_rf, framed, order_avro_body, produce,
    },
    support,
};

const ORDER_AVRO: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;
const ORDER_JSON: &str = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}"#;
const ORDER_PROTOBUF: &str = "syntax = \"proto3\"; message Order { int64 id = 1; }";

struct RegistryNode {
    url: String,
    primary: watch::Receiver<PrimaryState>,
    election_cancel: CancellationToken,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl RegistryNode {
    async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}

fn registry_config(bootstrap: &str, url: &str, id: usize) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 3,
        client_id: format!("m19-registry-{id}"),
        advertised_url: url.into(),
        group_id: "m19-schema-registry".into(),
        leader_eligibility: true,
        runtime: RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    }
}

async fn start_registry(
    bootstrap: &str,
    id: usize,
    listener: Option<tokio::net::TcpListener>,
) -> RegistryNode {
    let listener = match listener {
        Some(listener) => listener,
        None => tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    };
    let url = format!("http://{}", listener.local_addr().unwrap());
    let config = registry_config(bootstrap, &url, id);
    let cancel = CancellationToken::new();
    let election_cancel = cancel.child_token();
    let store = KafkaStore::start(&config, cancel.clone()).await.unwrap();
    let primary = Election::start(&config, election_cancel.clone())
        .await
        .unwrap();
    store.install_primary(primary.clone());
    let app = rest::router_with_forwarding(
        AppState { store },
        ForwardState {
            primary: primary.clone(),
            http: reqwest::Client::new(),
            node_id: url.clone(),
            forward_max_body: config.runtime.forward_max_body,
        },
    );
    let serve_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .unwrap();
    });
    RegistryNode {
        url,
        primary,
        election_cancel,
        cancel,
        task,
    }
}

async fn wait_for_primary(nodes: &mut [RegistryNode]) -> usize {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(index) = nodes
                .iter()
                .position(|node| node.primary.borrow().is_primary)
            {
                return index;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("registry primary election")
}

async fn register(client: &reqwest::Client, url: &str, subject: &str, body: Value) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = tokio::time::timeout_at(
            deadline,
            client
                .post(format!("{url}/subjects/{subject}/versions"))
                .json(&body)
                .send(),
        )
        .await
        .expect("registry registration deadline")
        .unwrap();
        let status = response.status();
        if status.is_success() {
            let response_body: Value = tokio::time::timeout_at(deadline, response.json())
                .await
                .expect("registry response deadline")
                .unwrap();
            return u32::try_from(response_body["id"].as_u64().expect("schema id")).unwrap();
        }
        let response_body = tokio::time::timeout_at(deadline, response.text())
            .await
            .expect("registry response deadline")
            .unwrap_or_default();
        assert!(
            tokio::time::Instant::now() < deadline,
            "registry returned {status}: {response_body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_schema(client: &reqwest::Client, url: &str, id: u32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if client
                .get(format!("{url}/schemas/ids/{id}"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("schema replication deadline");
}

async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    client
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn fetch_values(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    count: i64,
) -> Vec<Option<Bytes>> {
    broker.wait_until_high_watermark(topic, 0, count).await;
    let response = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 1_000,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let partition = &response.responses[0].partitions[0];
    assert!(partition.error_code == 0, "fetch failed: {partition:?}");
    partition
        .records
        .as_ref()
        .unwrap()
        .as_v2()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.records.iter().map(|record| record.value.clone()))
        .collect()
}

async fn client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("m19-live-registry")
        .build()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[allow(clippy::too_many_lines)] // Keep the live failover lifecycle in one acceptance test.
async fn rf_three_validation_survives_registry_and_broker_failover() {
    support::init_tracing();

    let registry_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let registry_url = format!("http://{}", registry_listener.local_addr().unwrap());
    let mut cluster = support::start_n_node_with(3, |_, config| {
        config.schema_validator = Some(Arc::new(
            SchemaValidator::new(registry_url.clone(), false, 64, minutes(5), secs(2)).unwrap(),
        ));
    })
    .await
    .unwrap();
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    // The second broker is neither the bootstrap controller nor the first
    // replica selected for the registry log, so registry clients must follow
    // metadata rather than assuming their seed is the leader.
    let bootstrap = cluster[1].0.listen_addr().to_string();

    let mut registries = vec![
        start_registry(&bootstrap, 1, Some(registry_listener)).await,
        start_registry(&bootstrap, 2, None).await,
    ];
    check!(registries[0].url == registry_url);
    let primary = wait_for_primary(&mut registries).await;
    let secondary = 1 - primary;

    // Register through the secondary so the first write also proves forwarding
    // to the elected primary and replication through the RF=3 `_schemas` log.
    let http = reqwest::Client::new();
    let avro_id = register(
        &http,
        &registries[secondary].url,
        "avro-value",
        serde_json::json!({"schema": ORDER_AVRO}),
    )
    .await;
    let json_id = register(
        &http,
        &registries[secondary].url,
        "json-value",
        serde_json::json!({"schemaType": "JSON", "schema": ORDER_JSON}),
    )
    .await;
    let protobuf_id = register(
        &http,
        &registries[secondary].url,
        "protobuf-value",
        serde_json::json!({"schemaType": "PROTOBUF", "schema": ORDER_PROTOBUF}),
    )
    .await;
    let wrong_subject_id = register(
        &http,
        &registries[secondary].url,
        "somewhere-else-value",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Elsewhere","fields":[{"name":"id","type":"string"}]}"#
        }),
    )
    .await;
    register(
        &http,
        &registries[secondary].url,
        "order-base",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#
        }),
    )
    .await;
    let referenced_id = register(
        &http,
        &registries[secondary].url,
        "referenced-value",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Envelope","fields":[{"name":"base","type":"Base"}]}"#,
            "references": [{"name":"Base","subject":"order-base","version":1}]
        }),
    )
    .await;
    let referenced = get_json(
        &http,
        &format!("{}/schemas/ids/{referenced_id}", registries[secondary].url),
    )
    .await;
    check!(referenced["references"][0]["subject"] == "order-base");

    let bootstrap_client = client(&bootstrap).await;
    let validation = &[
        ("schema.validation.value", "true"),
        ("schema.validation.mode", "full"),
    ];
    let avro_topic = create_topic_rf(&cluster[0].0, &bootstrap_client, "avro", validation, 3).await;
    let json_topic = create_topic_rf(&cluster[0].0, &bootstrap_client, "json", validation, 3).await;
    let protobuf_topic =
        create_topic_rf(&cluster[0].0, &bootstrap_client, "protobuf", validation, 3).await;
    let control_topic = create_topic_rf(&cluster[0].0, &bootstrap_client, "control", &[], 3).await;

    let leader_id = cluster[0]
        .0
        .partition_leader_for_test("avro", 0)
        .expect("avro leader");
    let leader_index = cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() == leader_id)
        .unwrap();
    let leader_client = client(&cluster[leader_index].0.listen_addr().to_string()).await;

    for (topic, topic_id, value) in [
        ("avro", avro_topic, framed(avro_id, &order_avro_body())),
        ("json", json_topic, framed(json_id, br#"{"id":1}"#)),
        (
            "protobuf",
            protobuf_topic,
            // Confluent's single-top-level-message index is one zero byte;
            // field 1 then carries the varint value 1.
            framed(protobuf_id, &[0, 0x08, 0x01]),
        ),
    ] {
        let response = produce(
            &leader_client,
            topic,
            topic_id,
            batch_with_value(Some(value)),
        )
        .await;
        check!(response.error_code == 0, "{topic}: {response:?}");
    }

    // Warm-cache acceptance and every required rejection keep the leader LEO
    // exact; no error response is allowed to hide an append.
    let warm = produce(
        &leader_client,
        "avro",
        avro_topic,
        batch_with_value(Some(framed(avro_id, &order_avro_body()))),
    )
    .await;
    check!(warm.error_code == 0, "{warm:?}");
    let control = produce(
        &leader_client,
        "control",
        control_topic,
        batch_with_value(Some(Bytes::from_static(b"unframed-control"))),
    )
    .await;
    check!(control.error_code == 0, "{control:?}");
    let tombstone = produce(&leader_client, "avro", avro_topic, batch_with_value(None)).await;
    check!(tombstone.error_code == 0, "{tombstone:?}");

    check!(
        fetch_values(
            &cluster[leader_index].0,
            &leader_client,
            "avro",
            avro_topic,
            3
        )
        .await
            == vec![
                Some(framed(avro_id, &order_avro_body())),
                Some(framed(avro_id, &order_avro_body())),
                None,
            ]
    );
    let control_leader_id = cluster[0]
        .0
        .partition_leader_for_test("control", 0)
        .expect("control leader");
    let control_leader = cluster
        .iter()
        .find(|(broker, _, _)| broker.node_id() == control_leader_id)
        .unwrap();
    let control_client = client(&control_leader.0.listen_addr().to_string()).await;
    check!(
        fetch_values(
            &control_leader.0,
            &control_client,
            "control",
            control_topic,
            1,
        )
        .await
            == vec![Some(Bytes::from_static(b"unframed-control"))]
    );
    for (case, invalid) in [
        ("framing", Bytes::from_static(b"not-confluent-framing")),
        ("unknown id", framed(u32::MAX, &order_avro_body())),
        (
            "wrong subject",
            framed(wrong_subject_id, &order_avro_body()),
        ),
        (
            "body mismatch",
            framed(avro_id, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        ),
    ] {
        let before = cluster[leader_index]
            .0
            .local_log_end_offset("avro", 0)
            .unwrap();
        let response = produce(
            &leader_client,
            "avro",
            avro_topic,
            batch_with_value(Some(invalid)),
        )
        .await;
        check!(
            response.error_code == INVALID_RECORD,
            "{case}: {response:?}"
        );
        check!(
            cluster[leader_index].0.local_log_end_offset("avro", 0) == Some(before),
            "{case} moved the log end"
        );
    }

    for (broker, _, _) in &cluster {
        broker.wait_until_local_log_end_offset("avro", 0, 3).await;
    }

    // Remove the registry primary from its election session. A new schema is
    // registered through the successor and read through node zero, the URL the
    // brokers have retained throughout.
    registries[primary].election_cancel.cancel();
    let successor = 1 - primary;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !registries[successor].primary.borrow().is_primary {
            registries[successor].primary.changed().await.unwrap();
        }
    })
    .await
    .expect("registry primary failover");
    let evolved_id = register(
        &http,
        &registries[successor].url,
        "avro-value",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}"#
        }),
    )
    .await;
    wait_for_schema(&http, &registries[0].url, evolved_id).await;
    let preserved = get_json(
        &http,
        &format!("{}/subjects/referenced-value/versions/1", registries[0].url),
    )
    .await;
    check!(preserved["id"] == u64::from(referenced_id));
    check!(preserved["version"] == 1);
    check!(preserved["references"][0]["subject"] == "order-base");

    // Abruptly kill the data-partition leader, then continue on the elected
    // successor using the same topic and the registry's new schema id.
    let (victim, _victim_config, victim_dir) = cluster.remove(leader_index);
    victim.crash_for_test().await;
    let observer = &cluster[0].0;
    observer
        .wait_until_partition_leader_changed("avro", 0, krabka_broker::NodeId(leader_id))
        .await;
    let new_leader_id = observer.partition_leader_for_test("avro", 0).unwrap();
    let new_leader = cluster
        .iter()
        .find(|(broker, _, _)| broker.node_id() == new_leader_id)
        .unwrap();
    let failover_client = client(&new_leader.0.listen_addr().to_string()).await;
    let response = produce(
        &failover_client,
        "avro",
        avro_topic,
        batch_with_value(Some(framed(evolved_id, &[0x02, b'b', 0x00]))),
    )
    .await;
    check!(response.error_code == 0, "{response:?}");
    new_leader
        .0
        .wait_until_local_log_end_offset("avro", 0, 4)
        .await;

    // With every registry endpoint gone, fail-closed rejects a fresh id and
    // still leaves the post-failover leader's LEO unchanged.
    while let Some(registry) = registries.pop() {
        registry.stop().await;
    }
    let before = new_leader.0.local_log_end_offset("avro", 0).unwrap();
    let unavailable = produce(
        &failover_client,
        "avro",
        avro_topic,
        batch_with_value(Some(framed(evolved_id + 10_000, &order_avro_body()))),
    )
    .await;
    check!(unavailable.error_code == INVALID_RECORD, "{unavailable:?}");
    check!(new_leader.0.local_log_end_offset("avro", 0) == Some(before));

    for (broker, _, _) in cluster {
        broker.shutdown().await;
    }
    drop(victim_dir);
}
