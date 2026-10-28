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
#![cfg(test)]

mod support;

use {
    crate::support::{
        admin,
        configs::{controller_registration, feature_update},
        discovery::api_versions_request_for,
        sasl::{ScramAlgorithm, scram_upsertion},
        start_with_bound_listeners,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{
        BrokerConfig, BrokerHandle, NodeId, api_catalog::UnstableApiVersions, config::NodeRole,
    },
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_metadata::metadata_version,
    krabka_protocol::{
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
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            alter_user_scram_credentials_response::{
                AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
            },
            api_versions_request::ApiVersionsRequest,
            api_versions_response::ApiVersionsResponse,
            assign_replicas_to_dirs_request::{
                AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
            },
            assign_replicas_to_dirs_response::{
                AssignReplicasToDirsResponse, DirectoryData as RespDirectoryData,
                PartitionData as RespPartitionData, TopicData as RespTopicData,
            },
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_heartbeat_response::BrokerHeartbeatResponse,
            broker_registration_request::{
                BrokerRegistrationRequest, Feature as RegistrationFeature,
                Listener as RegistrationListener,
            },
            create_delegation_token_request::CreateDelegationTokenRequest,
            create_delegation_token_response::CreateDelegationTokenResponse,
            create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
            create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
            describe_client_quotas_request,
            describe_user_scram_credentials_request::{
                DescribeUserScramCredentialsRequest, UserName,
            },
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
            unregister_controller_request::UnregisterControllerRequest,
            update_features_request::UpdateFeaturesRequest,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        time::Duration,
    },
    tokio::time,
};

/// Kafka's `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, which
/// `ControllerApis.allowTokenRequests` answers for a token RPC that arrives
/// over a PLAINTEXT connection without forwarding.
const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;

/// `DelegationTokenManager.ERROR_TIMESTAMP`, the timestamp Kafka writes into
/// every refused token response.
const TOKEN_ERROR_TIMESTAMP: i64 = -1;

/// `SCRAM-SHA-256` as KIP-554 numbers the mechanisms on the wire.
const SCRAM_SHA_256: i8 = 1;

/// Kafka's `INVALID_REPLICATION_FACTOR`, which `CreateTopics` answers when the
/// registered broker set cannot carry the requested replication factor.
const INVALID_REPLICATION_FACTOR: i16 = 38;

/// A running node and the directory that backs it.
struct ControllerNode {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Start a one-node broker whose controller listener is reachable on its own
/// port, and return the handle. Both listeners are bound before the broker
/// starts so the test knows the ports without racing the bind.
async fn start_broker() -> ControllerNode {
    start_node(&[NodeRole::Controller, NodeRole::Broker], |_| {}).await
}

/// [`start_broker`], serving what krabka implements from Kafka trunk:
/// `unstable.api.versions.enable` and `unstable.feature.versions.enable` on.
async fn start_trunk_broker() -> ControllerNode {
    start_node(&[NodeRole::Controller, NodeRole::Broker], enable_trunk).await
}

async fn start_surface(trunk: bool) -> ControllerNode {
    if trunk {
        start_trunk_broker().await
    } else {
        start_broker().await
    }
}

fn enable_trunk(config: &mut BrokerConfig) {
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
}

/// The same node, with the `process.roles` the case needs and `configure`
/// applied last.
async fn start_node(roles: &[NodeRole], configure: fn(&mut BrokerConfig)) -> ControllerNode {
    let (broker, dir) = start_with_bound_listeners(|config| {
        config.roles = roles.to_vec();
        configure(config);
    })
    .await;
    ControllerNode { broker, dir }
}

/// An `ApiVersions` request that passes Kafka's `ApiVersionsRequest.isValid`:
/// from v3 the KIP-511 client software name and version must be set.
fn api_versions_request() -> ApiVersionsRequest { api_versions_request_for("krabka-test", "1.0") }

/// Dial the controller listener. `Connection::connect` runs the `ApiVersions`
/// bootstrap, so every later `send` negotiates against what this listener
/// advertises rather than against the client's own codec range.
async fn dial_controller(broker: &BrokerHandle) -> Connection {
    Connection::connect(broker.controller_addr(), ConnectionOptions {
        client_id: "controller-admin-surface".to_owned(),
        ..ConnectionOptions::default()
    })
    .await
    .expect("dial the controller listener")
}

/// The versions of one api key that a listener advertises, inclusive.
#[derive(Debug, PartialEq, Eq)]
struct VersionRange {
    min: i16,
    max: i16,
}

#[derive(Default)]
struct ControllerAdminScenario {
    node: Option<ControllerNode>,
    connection: Option<Connection>,
    versions: Option<ApiVersionsResponse>,
    created: Option<CreateTopicsResponse>,
    grown: Option<CreatePartitionsResponse>,
    deleted: Option<DeleteTopicsResponse>,
    minted: Option<CreateDelegationTokenResponse>,
    renewed: Option<RenewDelegationTokenResponse>,
    expired: Option<ExpireDelegationTokenResponse>,
    scram_altered: Option<AlterUserScramCredentialsResponse>,
    scram_described: Option<DescribeUserScramCredentialsResponse>,
    assigned: Option<AssignReplicasToDirsResponse>,
    unregistered: Vec<UnregisterControllerResponse>,
    alter_partition: Option<AlterPartitionResponse>,
    allocated: Option<AllocateProducerIdsResponse>,
    handshake: Option<SaslHandshakeResponse>,
    authenticate: Option<SaslAuthenticateResponse>,
    registration_outcomes: Vec<Outcome>,
    registration_heartbeats: Vec<BrokerHeartbeatResponse>,
    epoch_of_7: i64,
    topic_id: WireUuid,
}
#[rstest::fixture]
fn controller_admin() -> ControllerAdminScenario {
    let epoch_of_7 = -1;
    ControllerAdminScenario {
        epoch_of_7,
        ..Default::default()
    }
}
impl ControllerAdminScenario {
    fn broker(&self) -> &BrokerHandle { &self.node.as_ref().expect("controller node").broker }

    fn connection(&self) -> &Connection { self.connection.as_ref().expect("controller connection") }

    async fn shutdown(&mut self) {
        self.connection
            .take()
            .expect("controller connection")
            .close();
        let ControllerNode { broker, dir: _dir } = self.node.take().expect("controller node");
        broker.shutdown().await;
    }
}
#[rstest_bdd_macros::given(
    "a combined broker has an open plaintext controller connection with trunk APIs enabled {trunk}"
)]
async fn combined_controller_admin_connection(
    controller_admin: &mut ControllerAdminScenario,
    trunk: bool,
) {
    let node = start_surface(trunk).await;
    controller_admin.connection = Some(dial_controller(&node.broker).await);
    controller_admin.node = Some(node);
}
#[rstest_bdd_macros::given(
    "a controller-only node has an open plaintext controller connection and no registered data broker"
)]
async fn controller_only_admin_connection(controller_admin: &mut ControllerAdminScenario) {
    let node = start_node(&[NodeRole::Controller], |_| {}).await;
    controller_admin.connection = Some(dial_controller(&node.broker).await);
    controller_admin.node = Some(node);
}
#[rstest_bdd_macros::when("the client requests ApiVersions from the controller listener")]
async fn controller_api_versions(controller_admin: &mut ControllerAdminScenario) {
    controller_admin.versions = Some(
        controller_admin
            .connection()
            .send(api_versions_request())
            .await
            .expect("controller ApiVersions"),
    );
}
#[rstest_bdd_macros::then(
    "the controller advertises these exact Admin API version ranges and excludes DescribeClientQuotas"
)]
async fn controller_admin_version_ranges(
    controller_admin: &mut ControllerAdminScenario,
    datatable: Vec<Vec<String>>,
) {
    let mut selected = BTreeSet::new();
    let mut expected = BTreeMap::new();
    for cells in datatable.into_iter().skip(1) {
        let key = cells[0].parse::<i16>().expect("API key");
        selected.insert(key);
        if cells[1] != "absent" {
            expected.insert(key, VersionRange {
                min: cells[1].parse().expect("minimum version"),
                max: cells[2].parse().expect("maximum version"),
            });
        }
    }
    let response = controller_admin
        .versions
        .as_ref()
        .expect("ApiVersions response");
    let advertised = response
        .api_keys
        .iter()
        .filter(|api| selected.contains(&api.api_key))
        .map(|api| {
            (api.api_key, VersionRange {
                min: api.min_version,
                max: api.max_version,
            })
        })
        .collect::<BTreeMap<_, _>>();
    check!(advertised == expected);
    check!(
        !response
            .api_keys
            .iter()
            .any(|api| api.api_key == describe_client_quotas_request::API_KEY)
    );
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::then("the complete controller API key set is {keys}")]
async fn controller_listener_key_set(controller_admin: &mut ControllerAdminScenario, keys: String) {
    let expected = keys
        .split(',')
        .map(|key| key.parse::<i16>().expect("API key"))
        .collect::<BTreeSet<_>>();
    let advertised = controller_admin
        .versions
        .as_ref()
        .expect("ApiVersions response")
        .api_keys
        .iter()
        .map(|api| api.api_key)
        .collect::<BTreeSet<_>>();
    check!(advertised.difference(&expected).next().is_none());
    check!(expected.difference(&advertised).next().is_none());
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "the controller client creates one partition of controller-lifecycle at replication factor one"
)]
async fn create_controller_lifecycle_topic(controller_admin: &mut ControllerAdminScenario) {
    let response = controller_admin
        .connection()
        .send(create_topic_request(
            creatable_topic().name("controller-lifecycle".into()).call(),
        ))
        .await
        .expect("controller CreateTopics");
    controller_admin.topic_id = response
        .topics
        .first()
        .map(|topic| topic.topic_id)
        .unwrap_or_default();
    controller_admin.created = Some(response);
}
#[rstest_bdd_macros::then(
    "the complete create response has a nonzero topic UUID, one partition, replication factor one and nonempty effective configuration"
)]
fn controller_lifecycle_created(controller_admin: &ControllerAdminScenario) {
    let created = controller_admin
        .created
        .as_ref()
        .expect("CreateTopics response")
        .clone();
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
}
#[rstest_bdd_macros::when(
    "the controller client grows controller-lifecycle to three partitions with no manual assignments"
)]
async fn grow_controller_lifecycle_topic(controller_admin: &mut ControllerAdminScenario) {
    controller_admin.grown = Some(
        controller_admin
            .connection()
            .send(CreatePartitionsRequest {
                topics: vec![CreatePartitionsTopic {
                    name: "controller-lifecycle".into(),
                    count: 3,
                    assignments: None,
                    ..Default::default()
                }],
                timeout_ms: 5000,
                ..Default::default()
            })
            .await
            .expect("controller CreatePartitions"),
    );
}
#[rstest_bdd_macros::then(
    "the complete CreatePartitions response contains controller-lifecycle with success code zero"
)]
fn controller_lifecycle_grown(controller_admin: &ControllerAdminScenario) {
    let grown = controller_admin
        .grown
        .as_ref()
        .expect("CreatePartitions response")
        .clone();
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
}
#[rstest_bdd_macros::when("the controller client deletes controller-lifecycle by name")]
async fn delete_controller_lifecycle_topic(controller_admin: &mut ControllerAdminScenario) {
    controller_admin.deleted = Some(
        controller_admin
            .connection()
            .send(DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: Some("controller-lifecycle".into()),
                    ..Default::default()
                }],
                timeout_ms: 5000,
                ..Default::default()
            })
            .await
            .expect("controller DeleteTopics"),
    );
}
#[rstest_bdd_macros::then(
    "the complete DeleteTopics response succeeds and carries the UUID minted by CreateTopics"
)]
async fn controller_lifecycle_deleted(controller_admin: &mut ControllerAdminScenario) {
    let deleted = controller_admin
        .deleted
        .as_ref()
        .expect("DeleteTopics response")
        .clone();
    let expected_delete = DeleteTopicsResponse {
        throttle_time_ms: 0,
        responses: vec![DeletableTopicResult {
            name: Some("controller-lifecycle".into()),
            // A deleted topic's row carries its topic id, as Kafka's
            // `ControllerApis.deleteTopics` answers it (#634).
            topic_id: controller_admin.topic_id,
            error_code: 0,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    check!(deleted == expected_delete);
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "the anonymous plaintext client creates a token with lifetime -1, renews HMAC not-a-token for -1ms and expires it after -1ms"
)]
async fn controller_plaintext_token_requests(controller_admin: &mut ControllerAdminScenario) {
    let connection = controller_admin.connection();
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
    controller_admin.minted = Some(created);
    controller_admin.renewed = Some(renewed);
    controller_admin.expired = Some(expired);
}
#[rstest_bdd_macros::then(
    "all three complete token responses refuse with code 64, timestamps -1 and anonymous owner and requester"
)]
async fn controller_plaintext_token_responses(controller_admin: &mut ControllerAdminScenario) {
    let created = controller_admin
        .minted
        .as_ref()
        .expect("mint response")
        .clone();
    let renewed = controller_admin
        .renewed
        .as_ref()
        .expect("renew response")
        .clone();
    let expired = controller_admin
        .expired
        .as_ref()
        .expect("expire response")
        .clone();
    check!(
        created
            == CreateDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                principal_type: "User".into(),
                principal_name: "ANONYMOUS".into(),
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "ANONYMOUS".into(),
                issue_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                max_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    check!(
        renewed
            == RenewDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    check!(
        expired
            == ExpireDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "the controller client stores alice's SCRAM-SHA-256 credential at 8192 iterations with salt-bytes and salted-password-bytes"
)]
async fn controller_scram_upsert(controller_admin: &mut ControllerAdminScenario) {
    let connection = controller_admin.connection();
    let altered = connection
        .send(AlterUserScramCredentialsRequest {
            upsertions: vec![
                scram_upsertion()
                    .mechanism(ScramAlgorithm::Sha256)
                    .iterations(8_192)
                    .salt(Bytes::from_static(b"salt-bytes"))
                    .salted_password(Bytes::from_static(b"salted-password-bytes"))
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("AlterUserScramCredentials over the controller listener");

    controller_admin.scram_altered = Some(altered);
}
#[rstest_bdd_macros::then(
    "the complete SCRAM alteration response contains only alice with success code zero"
)]
fn controller_scram_upsert_response(controller_admin: &ControllerAdminScenario) {
    let altered = controller_admin
        .scram_altered
        .as_ref()
        .expect("SCRAM alteration response")
        .clone();
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
}
#[rstest_bdd_macros::when("the same controller connection describes alice's SCRAM credentials")]
async fn controller_scram_read(controller_admin: &mut ControllerAdminScenario) {
    let connection = controller_admin.connection();
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
    controller_admin.scram_described = Some(described);
}
#[rstest_bdd_macros::then(
    "the complete credential description reports alice with SCRAM-SHA-256 and 8192 iterations"
)]
async fn controller_scram_read_response(controller_admin: &mut ControllerAdminScenario) {
    check!(
        controller_admin
            .scram_described
            .as_ref()
            .expect("SCRAM description")
            == &alice_scram_credentials()
    );
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::given(
    "broker 1 is registered and topic controller-dirs has one partition with its captured topic UUID"
)]
async fn controller_directory_assignment_topic(controller_admin: &mut ControllerAdminScenario) {
    let created = controller_admin
        .connection()
        .send(create_topic_request(
            creatable_topic().name("controller-dirs".into()).call(),
        ))
        .await
        .expect("create directory assignment topic");
    assert!(let [topic] = created.topics.as_slice());
    controller_admin.topic_id = topic.topic_id;
}
#[rstest_bdd_macros::when(
    "broker 1 assigns controller-dirs partition 0 to directory UUID 07070707070707070707070707070707 at its registered epoch"
)]
async fn controller_assign_directory(controller_admin: &mut ControllerAdminScenario) {
    let epoch = controller_admin
        .broker()
        .controller_image_for_test()
        .broker(NodeId(1))
        .expect("broker 1 registered")
        .broker_epoch;
    let request = AssignReplicasToDirsRequest {
        broker_id: 1,
        broker_epoch: epoch,
        directories: vec![DirectoryData {
            id: WireUuid([7; 16]),
            topics: vec![TopicData {
                topic_id: controller_admin.topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    controller_admin.assigned = Some(
        controller_admin
            .connection()
            .send(request)
            .await
            .expect("controller directory assignment"),
    );
}
#[rstest_bdd_macros::then(
    "the complete directory assignment response echoes directory 07070707070707070707070707070707, topic UUID and partition 0 with success at both levels"
)]
async fn controller_directory_assignment_response(controller_admin: &mut ControllerAdminScenario) {
    const LOG_DIR_ID: WireUuid = WireUuid([7; 16]);
    let topic_id = controller_admin.topic_id;
    let assigned = controller_admin
        .assigned
        .as_ref()
        .expect("directory assignment response")
        .clone();
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
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::given(
    "controller 7 is registered and metadata.version is upgraded to controller unregistration level 33"
)]
async fn controller_unregistration_level(controller_admin: &mut ControllerAdminScenario) {
    controller_admin
        .broker()
        .submit_metadata_record_for_test(controller_registration(NodeId(7)))
        .await
        .expect("controller registration");
    let upgraded = controller_admin
        .connection()
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update(
                "metadata.version",
                metadata_version::CONTROLLER_UNREGISTRATION_MIN_LEVEL,
                1,
            )],
            ..Default::default()
        })
        .await
        .expect("controller feature upgrade");
    check!(upgraded.error_code == 0, "{upgraded:?}");
}
#[rstest_bdd_macros::when(
    "the controller client unregisters controller 7 and then unknown controller 9"
)]
async fn controller_unregister_requests(controller_admin: &mut ControllerAdminScenario) {
    for controller_id in [7, 9] {
        let answer = controller_admin
            .connection()
            .send(UnregisterControllerRequest {
                controller_id,
                ..Default::default()
            })
            .await
            .expect("unregister controller");
        controller_admin.unregistered.push(answer);
    }
}
#[rstest_bdd_macros::then(
    "the complete unregister responses are success followed by CONTROLLER_ID_NOT_REGISTERED for 9 and controller 7 is absent"
)]
async fn controller_unregister_responses(controller_admin: &mut ControllerAdminScenario) {
    check!(
        controller_admin.unregistered
            == admin::unregister_controller_answers()
                .controller_id(9)
                .call()
    );
    check!(
        controller_admin
            .broker()
            .controller_image_for_test()
            .controller(NodeId(7))
            .is_none()
    );
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "the controller-only client creates controller-only-placement with one partition and replication factor one"
)]
async fn controller_only_topic_creation(controller_admin: &mut ControllerAdminScenario) {
    controller_admin.created = Some(
        controller_admin
            .connection()
            .send(create_topic_request(
                creatable_topic()
                    .name("controller-only-placement".into())
                    .call(),
            ))
            .await
            .expect("controller-only CreateTopics"),
    );
}
#[rstest_bdd_macros::then(
    "the complete create response refuses placement with INVALID_REPLICATION_FACTOR code 38, no topic UUID and empty configs"
)]
async fn controller_only_placement_response(controller_admin: &mut ControllerAdminScenario) {
    let created = controller_admin
        .created
        .as_ref()
        .expect("controller-only creation response")
        .clone();
    check!(
        created
            == CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: "controller-only-placement".into(),
                    topic_id: WireUuid([0; 16]),
                    error_code: INVALID_REPLICATION_FACTOR,
                    error_message: Some(
                        "Unable to replicate the partition 1 time(s): All brokers are currently \
                         fenced, or have all their log directories cordoned."
                            .into(),
                    ),
                    num_partitions: -1,
                    replication_factor: -1,
                    // Kafka never sets `Configs` on an error row, so it keeps
                    // the generated default, an empty list.
                    configs: Some(Vec::new()),
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "Kafka brokers send these ordered registrations and heartbeat accepted incarnation a after each registration"
)]
async fn controller_broker_registrations(
    controller_admin: &mut ControllerAdminScenario,
    datatable: Vec<Vec<String>>,
) {
    for cells in datatable.into_iter().skip(1) {
        let registration = Registration {
            broker_id: cells[0].parse().expect("broker ID"),
            incarnation: u128::from_str_radix(&cells[1], 16).expect("incarnation UUID"),
            port: cells[2].parse().expect("broker port"),
            log_dirs: cells[3]
                .split(',')
                .filter(|id| *id != "none")
                .map(|id| id.parse().expect("directory UUID"))
                .collect(),
            with_metadata_version: cells[4].parse().expect("metadata.version support"),
        };
        let wait: u64 = cells[5].parse().expect("delay before registration");
        if wait > 0 {
            time::sleep(Duration::from_millis(wait)).await;
        }
        let request = {
            let image = controller_admin.broker().controller_image_for_test();
            registration.request(&image)
        };
        let answer = controller_admin
            .connection()
            .send(request)
            .await
            .expect("controller BrokerRegistration");
        let node = NodeId(u64::try_from(registration.broker_id).expect("positive broker ID"));
        let epoch = match answer.broker_epoch {
            -1 => EpochAnswer::Refused,
            epoch if node == NodeId(7) && epoch == controller_admin.epoch_of_7 => EpochAnswer::Kept,
            _ => EpochAnswer::New,
        };
        let registered = if answer.error_code == 0 {
            controller_admin
                .broker()
                .wait_for_image(|image| {
                    image
                        .broker(node)
                        .is_some_and(|registered| registered.broker_epoch == answer.broker_epoch)
                })
                .await;
            controller_admin
                .broker()
                .controller_image_for_test()
                .broker(node)
                .map(|registered| RegisteredBroker {
                    port: registered.port,
                    fenced: registered.fenced,
                })
        } else {
            None
        };
        controller_admin.registration_outcomes.push(Outcome {
            error_code: answer.error_code,
            epoch,
            registered,
        });
        if answer.error_code != 0 || node != NodeId(7) {
            continue;
        }
        controller_admin.epoch_of_7 = answer.broker_epoch;
        if registration.incarnation == 0xa {
            let heartbeat = controller_admin
                .connection()
                .send(BrokerHeartbeatRequest {
                    broker_id: 7,
                    broker_epoch: answer.broker_epoch,
                    current_metadata_offset: answer.broker_epoch,
                    ..Default::default()
                })
                .await
                .expect("controller BrokerHeartbeat");
            controller_admin.registration_heartbeats.push(heartbeat);
        }
    }
}
#[rstest_bdd_macros::then(
    "the complete registration outcomes match these epoch relations and stored broker states, and both incarnation-a heartbeats succeed"
)]
async fn controller_broker_registration_outcomes(
    controller_admin: &mut ControllerAdminScenario,
    datatable: Vec<Vec<String>>,
) {
    let expected = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            let code: i16 = cells[0].parse().expect("registration error");
            if code != 0 {
                return refused(code);
            }
            accepted(
                match cells[1].as_str() {
                    "new" => EpochAnswer::New,
                    "kept" => EpochAnswer::Kept,
                    other => panic!("unknown epoch relation {other}"),
                },
                cells[2].parse().expect("stored broker port"),
                cells[3].parse().expect("stored fence"),
            )
        })
        .collect::<Vec<_>>();
    check!(controller_admin.registration_outcomes == expected);
    assert!(controller_admin.registration_heartbeats.len() == 2);
    for heartbeat in &controller_admin.registration_heartbeats {
        assert!(heartbeat.error_code == 0, "{heartbeat:?}");
    }
    controller_admin.shutdown().await;
}
#[rstest_bdd_macros::when(
    "broker 1 sends AlterPartition for unknown topic UUID 07070707070707070707070707070707 partition 0, broker 99 allocates producer IDs, and the plaintext connection attempts PLAIN SASL authentication"
)]
async fn controller_interbroker_requests(controller_admin: &mut ControllerAdminScenario) {
    let unknown_topic = WireUuid([7; 16]);
    controller_admin
        .broker()
        .wait_until_brokers_registered(1)
        .await;
    let broker_epoch = controller_admin
        .broker()
        .controller_image_for_test()
        .broker_epoch(krabka_metadata::NodeId(1))
        .expect("broker 1 registered");
    controller_admin.alter_partition = Some(
        controller_admin
            .connection()
            .send(AlterPartitionRequest {
                broker_id: 1,
                broker_epoch,
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
            .expect("controller AlterPartition"),
    );
    controller_admin.allocated = Some(
        controller_admin
            .connection()
            .send(AllocateProducerIdsRequest {
                broker_id: 99,
                broker_epoch: 0,
                ..Default::default()
            })
            .await
            .expect("controller AllocateProducerIds"),
    );
    controller_admin.handshake = Some(
        controller_admin
            .connection()
            .send(SaslHandshakeRequest {
                mechanism: "PLAIN".into(),
                ..Default::default()
            })
            .await
            .expect("controller SaslHandshake"),
    );
    controller_admin.authenticate = Some(
        controller_admin
            .connection()
            .send(SaslAuthenticateRequest {
                auth_bytes: Bytes::from_static(b"\0broker\0secret"),
                ..Default::default()
            })
            .await
            .expect("controller SaslAuthenticate"),
    );
}
#[rstest_bdd_macros::then(
    "the full inter-broker responses report UNKNOWN_TOPIC_ID 100 and STALE_BROKER_EPOCH 77, and both SASL responses report ILLEGAL_SASL_STATE 34"
)]
async fn controller_interbroker_responses(controller_admin: &mut ControllerAdminScenario) {
    let unknown_topic = WireUuid([7; 16]);
    let alter_partition = controller_admin
        .alter_partition
        .as_ref()
        .expect("AlterPartition response")
        .clone();
    let allocate = controller_admin
        .allocated
        .as_ref()
        .expect("AllocateProducerIds response")
        .clone();
    let handshake = controller_admin
        .handshake
        .as_ref()
        .expect("SaslHandshake response")
        .clone();
    let authenticate = controller_admin
        .authenticate
        .as_ref()
        .expect("SaslAuthenticate response")
        .clone();
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
    check!(
        allocate
            == AllocateProducerIdsResponse {
                error_code: STALE_BROKER_EPOCH,
                producer_id_start: -1,
                producer_id_len: 0,
                ..Default::default()
            }
    );
    check!(
        handshake
            == SaslHandshakeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }
    );
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
    controller_admin.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Released controller Admin version ranges match Kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_api_versions_advertises_the_kafka_controller_admin_surface(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Released controller listener advertises exactly Kafka controller keys"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_advertises_no_key_kafka_does_not(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Trunk controller Admin version ranges match Kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_api_versions_advertises_the_kafka_controller_admin_surface_trunk(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Trunk controller listener advertises exactly Kafka controller keys"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_advertises_no_key_kafka_does_not_trunk(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "The controller listener serves the complete topic lifecycle"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_topic_lifecycle(controller_admin: ControllerAdminScenario) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Anonymous plaintext controller token writes are refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_writing_delegation_token_apis(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "SCRAM credentials written through the controller listener can be described on the same connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_scram_write_path(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Replica directory assignment is served by the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_assign_replicas_to_dirs(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Trunk controller unregistration removes registered controllers and names unknown IDs"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_unregister_controller(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Controller-only nodes never host topic replicas"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_only_node_places_no_replica_on_itself(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Broker registration preserves incarnations and validates feature and directory ownership"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_registers_a_restarted_broker_as_kafka_does(
    controller_admin: ControllerAdminScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/admin_surface.feature",
    name = "Controller listener serves inter-broker RPCs and refuses SASL after plaintext authentication"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_inter_broker_and_sasl_apis(
    controller_admin: ControllerAdminScenario,
) {
}

/// Independent expected stored SCRAM response, including each empty tag collection.
fn alice_scram_credentials() -> DescribeUserScramCredentialsResponse {
    DescribeUserScramCredentialsResponse {
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
}

/// One `BrokerRegistration` a Kafka broker sends over the controller listener.
struct Registration {
    broker_id: i32,
    incarnation: u128,
    port: u16,
    log_dirs: Vec<u128>,
    with_metadata_version: bool,
}

impl Registration {
    fn request(&self, image: &krabka_metadata::MetadataImage) -> BrokerRegistrationRequest {
        BrokerRegistrationRequest {
            broker_id: self.broker_id,
            cluster_id: image.cluster_id().to_string(),
            incarnation_id: WireUuid(*uuid::Uuid::from_u128(self.incarnation).as_bytes()),
            // A broker with no `broker.rack` registers a null rack.
            rack: None,
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
                        || name.as_str() != metadata_version::METADATA_VERSION_FEATURE
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

/// The port and fence the image holds for a registered broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegisteredBroker {
    port: u16,
    fenced: bool,
}

/// What one registration step came to: the error code, the epoch, and the
/// port and fence the image then holds for the broker the step registered,
/// if the step was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    error_code: i16,
    epoch: EpochAnswer,
    registered: Option<RegisteredBroker>,
}

const fn refused(error_code: i16) -> Outcome {
    Outcome {
        error_code,
        epoch: EpochAnswer::Refused,
        registered: None,
    }
}

const fn accepted(epoch: EpochAnswer, port: u16, fenced: bool) -> Outcome {
    Outcome {
        error_code: 0,
        epoch,
        registered: Some(RegisteredBroker { port, fenced }),
    }
}

/// Kafka's `UNKNOWN_TOPIC_ID`.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// Kafka's `STALE_BROKER_EPOCH`: `ClusterControlManager.checkBrokerEpoch`
/// answers it for a broker with no registration, too.
const STALE_BROKER_EPOCH: i16 = 77;

/// Kafka's `ILLEGAL_SASL_STATE`.
const ILLEGAL_SASL_STATE: i16 = 34;

// BDD scenario entry points; acceptance steps are defined above.

use krabka_protocol::owned::unregister_controller_response::UnregisterControllerResponse;
