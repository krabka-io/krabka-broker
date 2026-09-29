//! Shared fixtures for the transactional suite: broker boot, topic creation,
//! transactional-producer initialisation, and record construction.
//!
//! `init_transaction` drives `FindCoordinator` and then retries
//! `InitProducerId` until the transaction coordinator is loaded, so a test does
//! not have to encode that readiness race itself.
//!
//! No broker creates `__transaction_state` or `__consumer_offsets` when it
//! starts. The boot helpers bring both coordinators up before they return.
//! The krabka producer does not retry `COORDINATOR_NOT_AVAILABLE` from
//! `FindCoordinator`, which Kafka's `TransactionManager` does.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_client_core::security::{ClientSecurity, SaslCredentials};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    init_producer_id_request::InitProducerIdRequest,
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::support;

pub async fn boot_single() -> (BrokerHandle, String, TempDir) {
    boot_single_with(|_| {}).await
}

/// [`boot_single`] under Kafka's `unstable.api.versions.enable`, for the
/// cases that drive Kafka trunk's `TxnOffsetCommit` v6 (KIP-1319).
pub async fn boot_single_trunk() -> (BrokerHandle, String, TempDir) {
    boot_single_with(|config| {
        config.features.unstable_api_versions =
            krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    })
    .await
}

async fn boot_single_with(configure: fn(&mut BrokerConfig)) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut config);
    let broker = Broker::start(config).await.unwrap();
    wait_until_coordinators_ready(&broker).await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Creates and loads `__transaction_state` and `__consumer_offsets`, as the
/// first client lookup of each does.
async fn wait_until_coordinators_ready(broker: &BrokerHandle) {
    broker.wait_until_transaction_coordinator_ready().await;
    broker.wait_until_group_coordinator_ready().await;
}

pub async fn create_topic(bootstrap: &str, name: &str) {
    create_topic_with_configs(bootstrap, name, Vec::new()).await;
}

pub async fn create_topic_with_segment_bytes(bootstrap: &str, name: &str, bytes: u64) {
    create_topic_with_configs(
        bootstrap,
        name,
        vec![CreatableTopicConfig {
            name: "internal.segment.bytes".into(),
            value: Some(bytes.to_string()),
            ..Default::default()
        }],
    )
    .await;
}

async fn create_topic_with_configs(
    bootstrap: &str,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap();
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

pub async fn init_transaction(
    client: &krabka_client_core::Client,
    transactional_id: &str,
) -> (i64, i16) {
    support::find_coordinator(client, support::KEY_TYPE_TRANSACTION, transactional_id).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(transactional_id.into()),
                transaction_timeout_ms: 60_000,
                ..Default::default()
            })
            .await
            .unwrap();
        if response.error_code == 0 {
            return (response.producer_id, response.producer_epoch);
        }
        // COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE and
        // NOT_COORDINATOR are the answers of a coordinator that is not ready.
        assert!(
            matches!(response.error_code, 14..=16),
            "InitProducerId: {response:?}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "InitProducerId coordinator did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Boots a single-broker cluster whose only listener is `SASL_PLAINTEXT`, with
/// `PLAIN` enabled and the given users provisioned. Returns the same
/// `(handle, bootstrap, dir)` triple as [`boot_single`].
pub fn boot_single_sasl(
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, String, TempDir)> {
    let dir = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    Box::pin(async move {
        let broker = Broker::start(cfg).await.unwrap();
        wait_until_coordinators_ready(&broker).await;
        let bootstrap = broker.listen_addr().to_string();
        (broker, bootstrap, dir)
    })
}

/// Client-side `SASL_PLAINTEXT` and `PLAIN` security for `(user, pass)`.
pub fn sasl_plain_security(user: &str, pass: &str) -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: user.to_string(),
            password: pass.to_string(),
        }),
        sasl_host: None,
    }
}

/// Creates the topic `name` with 1 partition over a SASL-authenticated admin
/// connection.
pub async fn create_topic_sasl(bootstrap: &str, name: &str, security: ClientSecurity) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .maybe_security(Some(security))
        .build()
        .await
        .unwrap();
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic_sasl {name}: error_code={}",
        cr.topics[0].error_code
    );
}

/// Builds a `ProducerRecord` for the given topic and string value.
pub fn rec(topic: &str, v: &str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from(v.to_string())),
        ..Default::default()
    }
}

pub async fn send_ok(producer: &Producer, record: ProducerRecord) {
    producer
        .send(record)
        .await
        .await
        .expect("producer delivery channel open")
        .expect("produce acknowledged");
}
