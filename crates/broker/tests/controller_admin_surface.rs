//! KIP-919: the Admin surface the controller listener answers.
//!
//! Apache Kafka tags every request schema with the listeners that accept it,
//! and `ControllerApis` answers exactly the ones tagged `controller`. Krabka
//! bridges that set onto the broker's own handler registry, so these cases
//! drive each api key the bridge routes over a raw controller connection and
//! assert the decoded response, plus the `ApiVersions` surface the same
//! listener advertises.
//!
//! The cases here cover the keys the bridge did not route before: the topic
//! lifecycle (`CreateTopics`, `CreatePartitions`, `DeleteTopics`), the three
//! writing delegation-token RPCs, the SCRAM write path, and
//! `AssignReplicasToDirs`. Of the keys the bridge already routed,
//! `client_admin_controller_bootstrap` drives `DescribeConfigs`, and the
//! official Kafka tools in `jvm_bootstrap_controller` drive `DescribeConfigs`,
//! `IncrementalAlterConfigs`, the three ACL RPCs and
//! `ListPartitionReassignments`.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, NodeId, config::NodeRole};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialUpsertion,
        },
        alter_user_scram_credentials_response::{
            AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
        },
        api_versions_request::ApiVersionsRequest,
        assign_replicas_to_dirs_request::{
            AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
        },
        assign_replicas_to_dirs_response::{
            AssignReplicasToDirsResponse, DirectoryData as RespDirectoryData,
            PartitionData as RespPartitionData, TopicData as RespTopicData,
        },
        create_delegation_token_request::CreateDelegationTokenRequest,
        create_delegation_token_response::CreateDelegationTokenResponse,
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `DELEGATION_TOKEN_AUTH_DISABLED`, which every token RPC answers
/// when the broker has no delegation-token secret key.
const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;

/// `SCRAM-SHA-256` as KIP-554 numbers the mechanisms on the wire.
const SCRAM_SHA_256: i8 = 1;

/// Kafka's `INVALID_REPLICATION_FACTOR`, which `CreateTopics` answers when the
/// registered broker set cannot carry the requested replication factor.
const INVALID_REPLICATION_FACTOR: i16 = 38;

/// Every api key a Kafka 4.x controller listener accepts, in key order.
///
/// Read off a live `mirror.gcr.io/apache/kafka:4.3.1` controller with a raw
/// `ApiVersions` v0 request, and identical to the set the `listeners` tag on
/// the request schemas in `kafka-clients-4.3.1.jar` marks `controller`.
const KAFKA_CONTROLLER_LISTENER_KEYS: [i16; 41] = [
    1, 17, 18, 19, 20, 29, 30, 31, 32, 33, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 67, 70, 73, 80, 81, 82,
];

/// The keys from [`KAFKA_CONTROLLER_LISTENER_KEYS`] krabka's controller
/// listener does not advertise, and why each is out of the Admin bridge's
/// reach:
///
/// - `SaslHandshake` (17) and `SaslAuthenticate` (36) are consumed by
///   `BrokerRaftHandshake` before the controller server sees the stream, so
///   the listener speaks them without listing them.
/// - `AlterPartition` (56) and `AllocateProducerIds` (67) have broker handlers,
///   but krabka's brokers send both to a controller's *broker* endpoint rather
///   than to its controller listener, which is where Kafka takes them. A
///   forwarded `AllocateProducerIds` does reach its handler, through
///   `Envelope` (58) rather than through a key of its own.
const KEYS_KRABKA_DOES_NOT_ANSWER: [i16; 4] = [17, 36, 56, 67];

/// Start a one-node broker whose controller listener is reachable on its own
/// port, and return the handle. Both listeners are bound before the broker
/// starts so the test knows the ports without racing the bind.
async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    start_node(&[NodeRole::Controller, NodeRole::Broker]).await
}

/// The same node, with the `process.roles` the case needs.
async fn start_node(roles: &[NodeRole]) -> (BrokerHandle, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind data listener");
    let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controller listener");
    let data_addr = data_listener.local_addr().expect("data addr");
    let controller_addr = controller_listener.local_addr().expect("controller addr");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
    config.roles = roles.to_vec();
    let broker =
        Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
            .await
            .expect("broker start");
    (broker, dir)
}

/// Dial the controller listener. `Connection::connect` runs the `ApiVersions`
/// bootstrap, so every later `send` negotiates against what this listener
/// advertises rather than against the client's own codec range.
async fn dial_controller(broker: &BrokerHandle) -> Connection {
    Connection::connect(
        broker.controller_addr(),
        ConnectionOptions {
            client_id: "controller-admin-surface".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("dial the controller listener")
}

/// The Admin api keys a Kafka 4.x controller listener accepts, paired with the
/// version range krabka speaks for each.
///
/// The keys are the ones a live `mirror.gcr.io/apache/kafka:4.3.1` controller
/// advertises in `ApiVersions` (the same set 4.0.0's request schemas tag
/// `controller`), minus the RPCs the controller listener serves without the
/// Admin bridge and the [`KEYS_KRABKA_DOES_NOT_ANSWER`] shortfall.
/// `DescribeClientQuotas` (48) is tagged `broker` only, so it is absent there,
/// absent here, and asserted absent below.
fn expected_admin_versions() -> std::collections::BTreeMap<i16, (i16, i16)> {
    macro_rules! range {
        ($($request:ident),+ $(,)?) => {
            std::collections::BTreeMap::from([$((
                krabka_protocol::owned::$request::API_KEY,
                (
                    krabka_protocol::owned::$request::MIN_VERSION,
                    krabka_protocol::owned::$request::MAX_VERSION,
                ),
            ),)+])
        };
    }

    range!(
        create_topics_request,
        delete_topics_request,
        describe_acls_request,
        create_acls_request,
        delete_acls_request,
        describe_configs_request,
        alter_configs_request,
        create_partitions_request,
        create_delegation_token_request,
        renew_delegation_token_request,
        expire_delegation_token_request,
        describe_delegation_token_request,
        elect_leaders_request,
        incremental_alter_configs_request,
        alter_partition_reassignments_request,
        list_partition_reassignments_request,
        alter_client_quotas_request,
        describe_user_scram_credentials_request,
        alter_user_scram_credentials_request,
        update_features_request,
        envelope_request,
        unregister_broker_request,
        assign_replicas_to_dirs_request,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_api_versions_advertises_the_kafka_controller_admin_surface() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let response = connection
        .send(ApiVersionsRequest::default())
        .await
        .expect("ApiVersions over the controller listener");
    connection.close();

    let expected = expected_admin_versions();
    let advertised: std::collections::BTreeMap<i16, (i16, i16)> = response
        .api_keys
        .iter()
        .filter(|api| expected.contains_key(&api.api_key))
        .map(|api| (api.api_key, (api.min_version, api.max_version)))
        .collect();

    check!(advertised == expected);
    // Tagged `broker` only in Kafka, so a Kafka controller does not offer it.
    check!(
        !response
            .api_keys
            .iter()
            .any(|api| api.api_key
                == krabka_protocol::owned::describe_client_quotas_request::API_KEY)
    );
    broker.shutdown().await;
}

/// The whole key set the controller listener advertises, measured against the
/// Kafka oracle rather than against krabka's own tables.
///
/// [`controller_api_versions_advertises_the_kafka_controller_admin_surface`]
/// only inspects the keys the Admin bridge routes, so it cannot see a key
/// krabka offers that no Kafka controller does, nor record which of Kafka's
/// the listener still does not answer. This case pins both directions: nothing
/// outside [`KAFKA_CONTROLLER_LISTENER_KEYS`] is advertised, and the shortfall
/// is exactly [`KEYS_KRABKA_DOES_NOT_ANSWER`]. Closing one of those gaps has to
/// come here and delete its entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_advertises_no_key_kafka_does_not() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let response = connection
        .send(ApiVersionsRequest::default())
        .await
        .expect("ApiVersions over the controller listener");
    connection.close();

    let advertised: std::collections::BTreeSet<i16> =
        response.api_keys.iter().map(|api| api.api_key).collect();
    let kafka: std::collections::BTreeSet<i16> =
        KAFKA_CONTROLLER_LISTENER_KEYS.iter().copied().collect();

    check!(advertised.difference(&kafka).copied().collect::<Vec<_>>() == Vec::<i16>::new());
    check!(
        kafka.difference(&advertised).copied().collect::<Vec<_>>()
            == KEYS_KRABKA_DOES_NOT_ANSWER.to_vec()
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_topic_lifecycle() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let created = connection
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "controller-lifecycle".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics over the controller listener");

    assert!(let [created_topic] = &created.topics[..]);
    // The topic id is minted per create, so the expectation borrows it back;
    // the check below pins it as non-nil, which is what the create promises.
    check!(created_topic.topic_id != WireUuid([0; 16]));
    // KIP-525 fills the row with the topic's whole effective configuration,
    // which the controller listener answers exactly as the broker listener
    // does. `admin_create_topics.rs` is what pins that list against
    // `DescribeConfigs`; here it is borrowed back like the topic id, so this
    // case stays about the lifecycle the controller listener serves.
    check!(
        created_topic
            .configs
            .as_ref()
            .is_some_and(|configs| !configs.is_empty())
    );
    check!(
        created
            == CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: "controller-lifecycle".into(),
                    topic_id: created_topic.topic_id,
                    error_code: 0,
                    error_message: None,
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: created_topic.configs.clone(),
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let grown = connection
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: "controller-lifecycle".into(),
                count: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreatePartitions over the controller listener");

    check!(
        grown
            == CreatePartitionsResponse {
                throttle_time_ms: 0,
                results: vec![CreatePartitionsTopicResult {
                    name: "controller-lifecycle".into(),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let deleted = connection
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some("controller-lifecycle".into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics over the controller listener");
    connection.close();

    check!(
        deleted
            == DeleteTopicsResponse {
                throttle_time_ms: 0,
                responses: vec![DeletableTopicResult {
                    name: Some("controller-lifecycle".into()),
                    // Deleting by name answers with the nil topic id, which is
                    // what the broker listener answers too: the routing this
                    // case covers hands the request to the same handler.
                    topic_id: WireUuid([0; 16]),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_writing_delegation_token_apis() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    // `for_tests` configures no delegation-token secret key, so each RPC takes
    // its "tokens are switched off" branch. That is the same answer Kafka gives
    // and it needs no key material to be deterministic.
    let created = connection
        .send(CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        })
        .await
        .expect("CreateDelegationToken over the controller listener");
    let renewed = connection
        .send(RenewDelegationTokenRequest {
            hmac: Bytes::from_static(b"not-a-token"),
            renew_period_ms: -1,
            ..Default::default()
        })
        .await
        .expect("RenewDelegationToken over the controller listener");
    let expired = connection
        .send(ExpireDelegationTokenRequest {
            hmac: Bytes::from_static(b"not-a-token"),
            expiry_time_period_ms: -1,
            ..Default::default()
        })
        .await
        .expect("ExpireDelegationToken over the controller listener");
    connection.close();

    check!(
        created
            == CreateDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_AUTH_DISABLED,
                ..Default::default()
            }
    );
    check!(
        renewed
            == RenewDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_AUTH_DISABLED,
                ..Default::default()
            }
    );
    check!(
        expired
            == ExpireDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_AUTH_DISABLED,
                ..Default::default()
            }
    );
    broker.shutdown().await;
}

/// `kafka-configs --bootstrap-controller --entity-type users --alter` is the
/// KIP-919 flow this covers: the SCRAM write lands through the controller
/// listener and the matching read on the same connection sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_scram_write_path() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let altered = connection
        .send(AlterUserScramCredentialsRequest {
            upsertions: vec![ScramCredentialUpsertion {
                name: "alice".into(),
                mechanism: SCRAM_SHA_256,
                iterations: 8_192,
                salt: Bytes::from_static(b"salt-bytes"),
                salted_password: Bytes::from_static(b"salted-password-bytes"),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterUserScramCredentials over the controller listener");

    check!(
        altered
            == AlterUserScramCredentialsResponse {
                throttle_time_ms: 0,
                results: vec![AlterUserScramCredentialsResult {
                    user: "alice".into(),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let described = connection
        .send(DescribeUserScramCredentialsRequest {
            users: Some(vec![UserName {
                name: "alice".into(),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("DescribeUserScramCredentials over the controller listener");
    connection.close();

    check!(
        described
            == DescribeUserScramCredentialsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                error_message: None,
                results: vec![DescribeUserScramCredentialsResult {
                    user: "alice".into(),
                    error_code: 0,
                    error_message: None,
                    credential_infos: vec![CredentialInfo {
                        mechanism: SCRAM_SHA_256,
                        iterations: 8_192,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}

/// `AssignReplicasToDirs` is the one routed key whose broker handler takes no
/// per-request context, so it also covers the bridge's plain-handler arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_assign_replicas_to_dirs() {
    const LOG_DIR_ID: WireUuid = WireUuid([7; 16]);

    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let created = connection
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "controller-dirs".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics over the controller listener");
    assert!(let [created_topic] = &created.topics[..]);
    let topic_id = created_topic.topic_id;

    let assigned = connection
        .send(AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![DirectoryData {
                id: LOG_DIR_ID,
                topics: vec![TopicData {
                    topic_id,
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AssignReplicasToDirs over the controller listener");
    connection.close();

    check!(
        assigned
            == AssignReplicasToDirsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                directories: vec![RespDirectoryData {
                    id: LOG_DIR_ID,
                    topics: vec![RespTopicData {
                        topic_id,
                        partitions: vec![RespPartitionData {
                            partition_index: 0,
                            error_code: 0,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}

/// KIP-919 puts `CreateTopics` and `CreatePartitions` on the controller
/// listener, so a controller-only node answers both -- and it hosts no
/// replicas. `process.roles` without `broker` means `register_broker` skips it,
/// so its image holds no broker at all, and placement has nowhere to put a
/// replica.
///
/// Kafka answers that with `INVALID_REPLICATION_FACTOR` ("the target
/// replication factor cannot be reached because only 0 broker(s) are
/// registered"). Substituting the local node instead would create a topic
/// whose only replica lives on a node that serves no partition, leaving
/// metadata nothing can ever serve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_only_node_places_no_replica_on_itself() {
    let (broker, _dir) = start_node(&[NodeRole::Controller]).await;
    let connection = dial_controller(&broker).await;

    let created = connection
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "controller-only-placement".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics over a controller-only listener");
    connection.close();

    check!(
        created
            == CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: "controller-only-placement".into(),
                    topic_id: WireUuid([0; 16]),
                    error_code: INVALID_REPLICATION_FACTOR,
                    error_message: None,
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}
