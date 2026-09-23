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

use std::time::Duration;

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, NodeId, config::NodeRole};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        allocate_producer_ids_response::AllocateProducerIdsResponse,
        alter_partition_request::{
            AlterPartitionRequest, BrokerState, PartitionData as AlterPartitionPartitionData,
            TopicData as AlterPartitionTopicData,
        },
        alter_partition_response::{
            AlterPartitionResponse, PartitionData as AlterPartitionResultPartition,
            TopicData as AlterPartitionResultTopic,
        },
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
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_registration_request::{
            BrokerRegistrationRequest, Feature as RegistrationFeature,
            Listener as RegistrationListener,
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
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
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

/// An `ApiVersions` request that passes Kafka's `ApiVersionsRequest.isValid`:
/// from v3 the KIP-511 client software name and version must be set.
fn api_versions_request() -> ApiVersionsRequest {
    ApiVersionsRequest {
        client_software_name: "krabka-test".into(),
        client_software_version: "1.0".into(),
        ..Default::default()
    }
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
/// Admin bridge.
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
        alter_partition_request,
        allocate_producer_ids_request,
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
        .send(api_versions_request())
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
/// krabka offers that no Kafka controller does, or a key of Kafka's that the
/// listener does not offer. This case pins both directions: the advertised set
/// is exactly [`KAFKA_CONTROLLER_LISTENER_KEYS`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_advertises_no_key_kafka_does_not() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;

    let response = connection
        .send(api_versions_request())
        .await
        .expect("ApiVersions over the controller listener");
    connection.close();

    let advertised: std::collections::BTreeSet<i16> =
        response.api_keys.iter().map(|api| api.api_key).collect();
    let kafka: std::collections::BTreeSet<i16> =
        KAFKA_CONTROLLER_LISTENER_KEYS.iter().copied().collect();

    check!(advertised.difference(&kafka).copied().collect::<Vec<_>>() == Vec::<i16>::new());
    check!(kafka.difference(&advertised).copied().collect::<Vec<_>>() == Vec::<i16>::new());
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

/// `AssignReplicasToDirs` is a context dispatch, so it also covers the
/// bridge's `ClusterAction`-authorized inter-broker arm; the default
/// `AllowAllAuthorizer` admits the connection's `ANONYMOUS` principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_assign_replicas_to_dirs() {
    const LOG_DIR_ID: WireUuid = WireUuid([7; 16]);

    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;
    let broker_epoch = broker
        .controller_image_for_test()
        .broker(NodeId(1))
        .expect("registered broker")
        .broker_epoch;

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
            broker_epoch,
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

/// Kafka's `DUPLICATE_BROKER_REGISTRATION`.
const DUPLICATE_BROKER_REGISTRATION: i16 = 101;

/// Kafka's `INVALID_REGISTRATION`.
const INVALID_REGISTRATION: i16 = 119;

/// One `BrokerRegistration` a Kafka broker sends over the controller listener.
struct Registration {
    broker_id: i32,
    incarnation: u128,
    port: u16,
    log_dirs: &'static [u128],
    with_metadata_version: bool,
}

impl Registration {
    const fn broker_7(incarnation: u128, port: u16) -> Self {
        Self {
            broker_id: 7,
            incarnation,
            port,
            log_dirs: &[7000],
            with_metadata_version: true,
        }
    }

    const fn broker_8() -> Self {
        Self {
            broker_id: 8,
            incarnation: 0x8a,
            port: 19_100,
            log_dirs: &[8000],
            with_metadata_version: true,
        }
    }

    fn request(&self, image: &krabka_metadata::MetadataImage) -> BrokerRegistrationRequest {
        BrokerRegistrationRequest {
            broker_id: self.broker_id,
            cluster_id: image.cluster_id().to_string(),
            incarnation_id: WireUuid(*uuid::Uuid::from_u128(self.incarnation).as_bytes()),
            listeners: vec![RegistrationListener {
                name: "PLAINTEXT".into(),
                host: "127.0.0.1".into(),
                port: self.port,
                security_protocol: 0,
                ..Default::default()
            }],
            // What a real broker's `SupportedFeatures` covers: every level
            // the cluster finalized.
            features: image
                .finalized_features()
                .iter()
                .filter(|(name, _)| {
                    self.with_metadata_version
                        || name.as_str()
                            != krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                })
                .map(|(name, level)| RegistrationFeature {
                    name: name.clone(),
                    min_supported_version: 0,
                    max_supported_version: *level,
                    ..Default::default()
                })
                .collect(),
            log_dirs: self
                .log_dirs
                .iter()
                .map(|id| WireUuid(*uuid::Uuid::from_u128(*id).as_bytes()))
                .collect(),
            ..Default::default()
        }
    }
}

/// How the broker epoch in a registration answer relates to the one broker 7
/// held before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EpochAnswer {
    Refused,
    New,
    Kept,
}

/// What one registration step came to: the error code, the epoch, and the
/// port the image then holds for the broker the step registered, if the
/// step was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    error_code: i16,
    epoch: EpochAnswer,
    registered_port: Option<u16>,
}

const fn refused(error_code: i16) -> Outcome {
    Outcome {
        error_code,
        epoch: EpochAnswer::Refused,
        registered_port: None,
    }
}

const fn accepted(epoch: EpochAnswer, port: u16) -> Outcome {
    Outcome {
        error_code: 0,
        epoch,
        registered_port: Some(port),
    }
}

/// krabka-io/krabka-broker#822: a Kafka broker that restarts rejoins over the
/// controller listener.
///
/// A Kafka broker picks a new incarnation id in every process. Kafka's
/// `ClusterControlManager.registerBroker` refuses that id with
/// `DUPLICATE_BROKER_REGISTRATION` only while the previous incarnation still
/// holds a heartbeat session, and registers it with a new broker epoch once
/// the session expires. It rewrites the record for the same incarnation and
/// keeps the epoch, and it validates `metadata.version` and the log
/// directories. Each step runs against the image the steps before it left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_registers_a_restarted_broker_as_kafka_does() {
    type Step = (&'static str, Registration, Option<Duration>, Outcome);
    let steps: Vec<Step> = vec![
        (
            "a first registration",
            Registration::broker_7(0xa, 19_092),
            None,
            accepted(EpochAnswer::New, 19_092),
        ),
        (
            "the same incarnation again, on a new port",
            Registration::broker_7(0xa, 19_093),
            None,
            accepted(EpochAnswer::Kept, 19_093),
        ),
        (
            "a new incarnation while the previous one heartbeats",
            Registration::broker_7(0xb, 19_094),
            None,
            refused(DUPLICATE_BROKER_REGISTRATION),
        ),
        (
            "the new incarnation once the session expired",
            Registration::broker_7(0xb, 19_094),
            // Longer than the two-second `heartbeat_timeout` of the test
            // configuration.
            Some(Duration::from_millis(2_500)),
            accepted(EpochAnswer::New, 19_094),
        ),
        (
            "no metadata.version feature",
            Registration {
                with_metadata_version: false,
                ..Registration::broker_8()
            },
            None,
            refused(INVALID_REGISTRATION),
        ),
        (
            "no log directory",
            Registration {
                log_dirs: &[],
                ..Registration::broker_8()
            },
            None,
            refused(INVALID_REGISTRATION),
        ),
        (
            "a log directory broker 7 registered",
            Registration {
                log_dirs: &[8000, 7000],
                ..Registration::broker_8()
            },
            None,
            refused(INVALID_REGISTRATION),
        ),
    ];

    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;
    let mut epoch_of_7 = -1;
    let mut expected = Vec::new();
    let mut outcomes = Vec::new();
    for (what, registration, wait, outcome) in steps {
        expected.push((what, outcome));
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
        let image = broker.controller_image_for_test();
        let answer = connection
            .send(registration.request(&image))
            .await
            .expect("BrokerRegistration over the controller listener");
        let node = NodeId(u64::try_from(registration.broker_id).expect("a broker id"));
        let epoch = match answer.broker_epoch {
            -1 => EpochAnswer::Refused,
            epoch if node == NodeId(7) && epoch == epoch_of_7 => EpochAnswer::Kept,
            _ => EpochAnswer::New,
        };
        let registered_port = if answer.error_code == 0 {
            broker
                .wait_for_image(|image| {
                    image
                        .broker(node)
                        .is_some_and(|registered| registered.broker_epoch == answer.broker_epoch)
                })
                .await;
            broker
                .controller_image_for_test()
                .broker(node)
                .map(|registered| registered.port)
        } else {
            None
        };
        outcomes.push((
            what,
            Outcome {
                error_code: answer.error_code,
                epoch,
                registered_port,
            },
        ));
        if answer.error_code != 0 || node != NodeId(7) {
            continue;
        }
        epoch_of_7 = answer.broker_epoch;
        if registration.incarnation == 0xa {
            // The first incarnation heartbeats, so it holds a session.
            let heartbeat = connection
                .send(BrokerHeartbeatRequest {
                    broker_id: 7,
                    broker_epoch: answer.broker_epoch,
                    current_metadata_offset: answer.broker_epoch,
                    ..Default::default()
                })
                .await
                .expect("BrokerHeartbeat over the controller listener");
            assert!(heartbeat.error_code == 0, "{what}: {heartbeat:?}");
        }
    }
    connection.close();
    broker.shutdown().await;

    check!(outcomes == expected);
}

/// Kafka's `UNKNOWN_TOPIC_ID`.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// Kafka's `BROKER_ID_NOT_REGISTERED`.
const BROKER_ID_NOT_REGISTERED: i16 = 102;

/// Kafka's `ILLEGAL_SASL_STATE`.
const ILLEGAL_SASL_STATE: i16 = 34;

/// A Kafka broker sends `AlterPartition` and `AllocateProducerIds` to the
/// active controller over its controller listener, and `ControllerApis`
/// answers `SaslHandshake` and `SaslAuthenticate` there with
/// `ILLEGAL_SASL_STATE`. `Connection::send` negotiates each one off this
/// listener's `ApiVersions` table, so an unadvertised key fails before it is
/// sent. The requests go to the highest version both sides speak:
/// `AlterPartition` v3, `AllocateProducerIds` v0, `SaslHandshake` v1 and
/// `SaslAuthenticate` v2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_inter_broker_and_sasl_apis() {
    let (broker, _dir) = start_broker().await;
    let connection = dial_controller(&broker).await;
    let unknown_topic = WireUuid([7; 16]);

    let alter_partition = connection
        .send(AlterPartitionRequest {
            broker_id: 1,
            broker_epoch: -1,
            topics: vec![AlterPartitionTopicData {
                topic_id: unknown_topic,
                partitions: vec![AlterPartitionPartitionData {
                    partition_index: 0,
                    leader_epoch: 0,
                    new_isr_with_epochs: vec![BrokerState {
                        broker_id: 1,
                        broker_epoch: -1,
                        ..Default::default()
                    }],
                    partition_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterPartition over the controller listener");
    check!(
        alter_partition
            == AlterPartitionResponse {
                topics: vec![AlterPartitionResultTopic {
                    topic_id: unknown_topic,
                    partitions: vec![AlterPartitionResultPartition {
                        partition_index: 0,
                        error_code: UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );

    let allocate = connection
        .send(AllocateProducerIdsRequest {
            broker_id: 99,
            broker_epoch: 0,
            ..Default::default()
        })
        .await
        .expect("AllocateProducerIds over the controller listener");
    check!(
        allocate
            == AllocateProducerIdsResponse {
                error_code: BROKER_ID_NOT_REGISTERED,
                producer_id_start: -1,
                producer_id_len: 0,
                ..Default::default()
            }
    );

    let handshake = connection
        .send(SaslHandshakeRequest {
            mechanism: "PLAIN".into(),
            ..Default::default()
        })
        .await
        .expect("SaslHandshake over the controller listener");
    check!(
        handshake
            == SaslHandshakeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }
    );

    let authenticate = connection
        .send(SaslAuthenticateRequest {
            auth_bytes: Bytes::from_static(b"\0broker\0secret"),
            ..Default::default()
        })
        .await
        .expect("SaslAuthenticate over the controller listener");
    check!(
        authenticate
            == SaslAuthenticateResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(
                    "SaslAuthenticate request received after successful authentication".into()
                ),
                ..Default::default()
            }
    );

    connection.close();
    broker.shutdown().await;
}
