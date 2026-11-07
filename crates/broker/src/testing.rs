//! Builders for requests shared by integration and unit tests.

use {
    crate::{
        BrokerConfig, SslPrincipalMapper,
        barrier::marker::{BarrierMarker, build_barrier_batch},
        config::{InterBrokerCredentials, ListenerSpec},
        error::BrokerError,
    },
    krabka_authz::SimpleAclAuthorizer,
    krabka_protocol::{
        owned::{list_offsets_request, produce_request},
        primitives::uuid::Uuid,
        records,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        iter,
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::Arc,
    },
};

/// A test broker config with one SASL/PLAINTEXT loopback listener that takes
/// PLAIN logins, for a client that has to authenticate.
///
/// The listener also carries inter-broker traffic, which logs in as the first
/// of `users`. With no `super_users` every principal is allowed everything.
/// With some, the broker enforces ACLs and only they bypass them, so a request
/// shows which principal the broker authenticated.
#[bon::builder]
pub fn sasl_plain_broker_config(
    log_dir: PathBuf,
    /// The `(username, password)` pairs the listener accepts.
    users: &[(&str, &str)],
    /// The principal names, such as `alice`, that bypass ACLs.
    #[builder(default)]
    super_users: &[&str],
) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_owned(),
        bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        advertised: "127.0.0.1:0".to_owned(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: Some(vec![SaslMechanism::Plain]),
        principal_mapper: SslPrincipalMapper::default(),
    }];
    "SASL_PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    config.plain_credentials = users
        .iter()
        .map(|&(user, password)| (user.to_owned(), password.to_owned()))
        .collect();
    config.inter_broker_credentials =
        users
            .first()
            .map(|&(username, password)| InterBrokerCredentials::Plain {
                username: username.to_owned(),
                password: password.to_owned(),
            });
    if !super_users.is_empty() {
        config.super_users = super_users.iter().map(|&name| name.to_owned()).collect();
        // The broker's own heartbeat reaches its PLAINTEXT controller listener
        // as `ANONYMOUS`, and it needs `ClusterAction` to unfence.
        config.authorizer = Arc::new(SimpleAclAuthorizer::new(
            config
                .super_users
                .iter()
                .cloned()
                .chain(iter::once("ANONYMOUS".to_owned()))
                .collect(),
        ));
    }
    config
}

/// Build a barrier marker batch using the broker's production wire encoder.
///
/// # Errors
///
/// Returns an error if the marker cannot be encoded as a record batch.
pub fn barrier_marker_batch(
    group: &str,
    epoch: i64,
    triggered_at: i64,
    base_offset: i64,
) -> Result<records::RecordBatch, BrokerError> {
    build_barrier_batch(
        &BarrierMarker {
            group: group.to_owned(),
            epoch,
            triggered_at,
        },
        krabka_log::Offset(base_offset),
        0,
    )
}

/// Build the one-topic, one-partition Produce request shared by live broker
/// integration fixtures.
#[must_use]
pub fn single_partition_produce_request(
    topic: impl Into<String>,
    topic_id: uuid::Uuid,
    partition_index: i32,
    batch: records::RecordBatch,
    acks: i16,
    timeout_ms: i32,
) -> produce_request::ProduceRequest {
    use krabka_protocol::{
        owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        primitives::uuid::Uuid as WireUuid,
    };

    ProduceRequest {
        transactional_id: None,
        acks,
        timeout_ms,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id: WireUuid(*topic_id.as_bytes()),
            partition_data: vec![PartitionProduceData {
                index: partition_index,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Build one ordinary record batch for fixtures that need only its offset,
/// leader epoch, timestamp, and value.
#[bon::builder]
pub fn single_value_batch(
    value: bytes::Bytes,
    #[builder(default)] base_offset: i64,
    #[builder(default)] leader_epoch: i32,
    #[builder(default)] timestamp_ms: i64,
) -> records::RecordBatch {
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    RecordBatch {
        base_offset,
        partition_leader_epoch: leader_epoch,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: timestamp_ms,
        max_timestamp: timestamp_ms,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            offset_delta: 0,
            timestamp_delta: 0,
            key: None,
            value: Some(value),
            headers: vec![],
        }],
    }
}

/// Build the one-partition `ListOffsets` requests used by the broker and
/// restore round-trip suites.
#[bon::builder]
pub fn list_offsets_request(
    topic: String,
    partition_index: i32,
    timestamp: i64,
    #[builder(default)] current_leader_epoch: i32,
    #[builder(default)] isolation_level: i8,
) -> list_offsets_request::ListOffsetsRequest {
    use krabka_protocol::owned::list_offsets_request::{
        ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
    };

    ListOffsetsRequest {
        replica_id: -1,
        isolation_level,
        topics: vec![ListOffsetsTopic {
            name: topic,
            partitions: vec![ListOffsetsPartition {
                partition_index,
                current_leader_epoch,
                timestamp,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }
}

/// Build a Produce request whose topic contains one partition row per entry.
#[bon::builder]
pub fn produce_partitions_request(
    acks: i16,
    timeout_ms: i32,
    topic: String,
    topic_id: Uuid,
    partitions: Vec<produce_request::PartitionProduceData>,
) -> produce_request::ProduceRequest {
    use krabka_protocol::owned::produce_request::{ProduceRequest, TopicProduceData};

    ProduceRequest {
        acks,
        timeout_ms,
        topic_data: vec![TopicProduceData {
            name: topic,
            topic_id,
            partition_data: partitions,
            ..Default::default()
        }],
        ..Default::default()
    }
}
