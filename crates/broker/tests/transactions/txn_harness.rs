//! Shared fixtures for the transactional suite: broker boot, topic creation,
//! transactional-producer initialisation, and record construction.
//!
//! `init_transaction` drives `FindCoordinator` and then retries
//! `InitProducerId` until the transaction coordinator is loaded, so a test does
//! not have to encode that readiness race itself.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_client_core::security::{ClientSecurity, SaslCredentials};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

pub async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub async fn create_topic(bootstrap: &str, name: &str) {
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
    let coordinator = client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|entry| entry.error_code == 0),
        "FindCoordinator: {coordinator:?}"
    );

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
        assert!(
            response.error_code == 15 || response.error_code == 16,
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
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    Box::pin(async move {
        let broker = Broker::start(cfg).await.unwrap();
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
