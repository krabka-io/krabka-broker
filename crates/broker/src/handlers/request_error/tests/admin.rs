use {
    super::{TOPIC_ID, case, check, default_message, exception_message},
    crate::codes::ILLEGAL_SASL_STATE,
    bytes::Bytes,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            alter_client_quotas_request::{
                AlterClientQuotasRequest, EntityData as RequestEntity, EntryData as RequestEntry,
            },
            alter_client_quotas_response::{AlterClientQuotasResponse, EntityData, EntryData},
            alter_configs_request::{AlterConfigsRequest, AlterConfigsResource},
            alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
            alter_partition_reassignments_request::{
                AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
            },
            alter_partition_reassignments_response::{
                AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
                ReassignableTopicResponse,
            },
            alter_replica_log_dirs_request::{
                AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
            },
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
            alter_user_scram_credentials_request::{
                AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
            },
            alter_user_scram_credentials_response::{
                AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
            },
            create_acls_request::{AclCreation, CreateAclsRequest},
            create_acls_response::{AclCreationResult, CreateAclsResponse},
            create_delegation_token_request::CreateDelegationTokenRequest,
            create_delegation_token_response::CreateDelegationTokenResponse,
            create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
            create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
            delete_acls_response::{DeleteAclsFilterResult, DeleteAclsResponse},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
            describe_client_quotas_request::DescribeClientQuotasRequest,
            describe_client_quotas_response::DescribeClientQuotasResponse,
            describe_cluster_request::DescribeClusterRequest,
            describe_cluster_response::DescribeClusterResponse,
            describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
            describe_configs_response::{DescribeConfigsResponse, DescribeConfigsResult},
            describe_delegation_token_request::DescribeDelegationTokenRequest,
            describe_delegation_token_response::DescribeDelegationTokenResponse,
            describe_log_dirs_request::DescribeLogDirsRequest,
            describe_log_dirs_response::DescribeLogDirsResponse,
            describe_user_scram_credentials_request::{
                DescribeUserScramCredentialsRequest, UserName,
            },
            describe_user_scram_credentials_response::{
                DescribeUserScramCredentialsResponse, DescribeUserScramCredentialsResult,
            },
            elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
            elect_leaders_response::{
                ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
            },
            expire_delegation_token_request::ExpireDelegationTokenRequest,
            expire_delegation_token_response::ExpireDelegationTokenResponse,
            incremental_alter_configs_request::{
                AlterConfigsResource as IncrementalResource, IncrementalAlterConfigsRequest,
            },
            incremental_alter_configs_response::{
                AlterConfigsResourceResponse as IncrementalResourceResponse,
                IncrementalAlterConfigsResponse,
            },
            list_config_resources_request::ListConfigResourcesRequest,
            list_config_resources_response::ListConfigResourcesResponse,
            list_partition_reassignments_request::{
                ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
            },
            list_partition_reassignments_response::{
                ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
                OngoingTopicReassignment,
            },
            renew_delegation_token_request::RenewDelegationTokenRequest,
            renew_delegation_token_response::RenewDelegationTokenResponse,
            unregister_broker_request::UnregisterBrokerRequest,
            unregister_broker_response::UnregisterBrokerResponse,
            update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
            update_features_response::UpdateFeaturesResponse,
        },
        primitives::uuid::Uuid,
    },
};

/// The topic apis answer each requested topic, `DeleteTopics` naming topics by
/// name before v6 and by state from v6.
#[test]
fn topic_admin_apis_repeat_every_requested_topic() {
    let deleted = |name: Option<&str>, topic_id| DeletableTopicResult {
        name: name.map(str::to_owned),
        topic_id,
        error_code: ILLEGAL_SASL_STATE,
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::CreateTopics,
            7,
            CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "orders".to_owned(),
                    num_partitions: 3,
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(CreateTopicsResponse {
                topics: vec![CreatableTopicResult {
                    name: "orders".to_owned(),
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(exception_message()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteTopics,
            5,
            DeleteTopicsRequest {
                topic_names: vec!["orders".to_owned()],
                ..Default::default()
            },
            Some(DeleteTopicsResponse {
                responses: vec![deleted(Some("orders"), Uuid::ZERO)],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteTopics,
            6,
            DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: None,
                    topic_id: TOPIC_ID,
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DeleteTopicsResponse {
                responses: vec![deleted(None, TOPIC_ID)],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::CreatePartitions,
            3,
            CreatePartitionsRequest {
                topics: vec![CreatePartitionsTopic {
                    name: "orders".to_owned(),
                    count: 6,
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(CreatePartitionsResponse {
                results: vec![CreatePartitionsTopicResult {
                    name: "orders".to_owned(),
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(exception_message()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}

/// The ACL apis: one result per creation or filter.
#[test]
fn acl_apis() {
    let creation = AclCreationResult {
        error_code: ILLEGAL_SASL_STATE,
        error_message: Some(exception_message()),
        ..Default::default()
    };
    let filter = DeleteAclsFilterResult {
        error_code: ILLEGAL_SASL_STATE,
        error_message: Some(exception_message()),
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::CreateAcls,
            3,
            CreateAclsRequest {
                creations: vec![AclCreation::default(), AclCreation::default()],
                ..Default::default()
            },
            Some(CreateAclsResponse {
                results: vec![creation.clone(), creation],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteAcls,
            3,
            DeleteAclsRequest {
                filters: vec![DeleteAclsFilter::default()],
                ..Default::default()
            },
            Some(DeleteAclsResponse {
                filter_results: vec![filter],
                ..Default::default()
            }),
        ),
    ]);
}

/// `ElectLeaders` carries the top-level code only from v1, and answers a null
/// partition list with no results.
#[test]
fn elect_leaders_answers_by_version() {
    let request = ElectLeadersRequest {
        topic_partitions: Some(vec![TopicPartitions {
            topic: "orders".to_owned(),
            partitions: vec![0],
            ..Default::default()
        }]),
        ..Default::default()
    };
    let results = || {
        vec![ReplicaElectionResult {
            topic: "orders".to_owned(),
            partition_result: vec![PartitionResult {
                partition_id: 0,
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                ..Default::default()
            }],
            ..Default::default()
        }]
    };
    check(vec![
        case(
            ApiKey::ElectLeaders,
            0,
            request.clone(),
            Some(ElectLeadersResponse {
                replica_election_results: results(),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ElectLeaders,
            1,
            request,
            Some(ElectLeadersResponse {
                error_code: ILLEGAL_SASL_STATE,
                replica_election_results: results(),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ElectLeaders,
            2,
            ElectLeadersRequest {
                topic_partitions: None,
                ..Default::default()
            },
            Some(ElectLeadersResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// The reassignment apis carry the code at the top level and on each
/// requested partition.
#[test]
fn reassignment_apis() {
    check(vec![
        case(
            ApiKey::AlterPartitionReassignments,
            0,
            AlterPartitionReassignmentsRequest {
                topics: vec![ReassignableTopic {
                    name: "orders".to_owned(),
                    partitions: vec![ReassignablePartition {
                        partition_index: 1,
                        replicas: Some(vec![1, 2]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(AlterPartitionReassignmentsResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                responses: vec![ReassignableTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![ReassignablePartitionResponse {
                        partition_index: 1,
                        error_code: ILLEGAL_SASL_STATE,
                        error_message: Some(exception_message()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ListPartitionReassignments,
            0,
            ListPartitionReassignmentsRequest {
                topics: Some(vec![ListPartitionReassignmentsTopics {
                    name: "orders".to_owned(),
                    partition_indexes: vec![1, 2],
                    ..Default::default()
                }]),
                ..Default::default()
            },
            Some(ListPartitionReassignmentsResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                topics: vec![OngoingTopicReassignment {
                    name: "orders".to_owned(),
                    partitions: [1, 2]
                        .map(|partition_index| OngoingPartitionReassignment {
                            partition_index,
                            ..Default::default()
                        })
                        .into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}

/// The config apis answer each requested resource.
#[test]
fn config_apis_repeat_every_requested_resource() {
    check(vec![
        case(
            ApiKey::DescribeConfigs,
            4,
            DescribeConfigsRequest {
                resources: vec![DescribeConfigsResource {
                    resource_type: 2,
                    resource_name: "orders".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DescribeConfigsResponse {
                results: vec![DescribeConfigsResult {
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(default_message()),
                    resource_type: 2,
                    resource_name: "orders".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AlterConfigs,
            2,
            AlterConfigsRequest {
                resources: vec![AlterConfigsResource {
                    resource_type: 4,
                    resource_name: "1".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(AlterConfigsResponse {
                responses: vec![AlterConfigsResourceResponse {
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(exception_message()),
                    resource_type: 4,
                    resource_name: "1".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::IncrementalAlterConfigs,
            1,
            IncrementalAlterConfigsRequest {
                resources: vec![IncrementalResource {
                    resource_type: 2,
                    resource_name: "orders".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(IncrementalAlterConfigsResponse {
                responses: vec![IncrementalResourceResponse {
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(exception_message()),
                    resource_type: 2,
                    resource_name: "orders".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ListConfigResources,
            1,
            ListConfigResourcesRequest::default(),
            Some(ListConfigResourcesResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// The log-dir, quota and cluster apis.
#[test]
fn log_dir_quota_and_cluster_apis() {
    check(vec![
        case(
            ApiKey::AlterReplicaLogDirs,
            2,
            AlterReplicaLogDirsRequest {
                dirs: vec![
                    AlterReplicaLogDir {
                        path: "/a".to_owned(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".to_owned(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    AlterReplicaLogDir {
                        path: "/b".to_owned(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".to_owned(),
                            partitions: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            Some(AlterReplicaLogDirsResponse {
                results: [0, 1]
                    .map(|partition_index| AlterReplicaLogDirTopicResult {
                        topic_name: "orders".to_owned(),
                        partitions: vec![AlterReplicaLogDirPartitionResult {
                            partition_index,
                            error_code: ILLEGAL_SASL_STATE,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .into(),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeLogDirs,
            4,
            DescribeLogDirsRequest::default(),
            Some(DescribeLogDirsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeClientQuotas,
            1,
            DescribeClientQuotasRequest::default(),
            Some(DescribeClientQuotasResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                entries: None,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AlterClientQuotas,
            1,
            AlterClientQuotasRequest {
                entries: vec![RequestEntry {
                    entity: vec![RequestEntity {
                        entity_type: "user".to_owned(),
                        entity_name: Some("alice".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(AlterClientQuotasResponse {
                entries: vec![EntryData {
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(default_message()),
                    entity: vec![EntityData {
                        entity_type: "user".to_owned(),
                        entity_name: Some("alice".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeCluster,
            2,
            DescribeClusterRequest::default(),
            Some(DescribeClusterResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::UpdateFeatures,
            2,
            UpdateFeaturesRequest {
                feature_updates: vec![FeatureUpdateKey {
                    feature: "metadata.version".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(UpdateFeaturesResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::UnregisterBroker,
            0,
            UnregisterBrokerRequest::default(),
            Some(UnregisterBrokerResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                ..Default::default()
            }),
        ),
    ]);
}

/// The SCRAM credential apis. `DescribeUserScramCredentials` gets no response
/// for a null user list, and `AlterUserScramCredentials` answers each distinct
/// user once, sorted by UTF-16 code unit.
#[test]
fn scram_credential_apis() {
    let described = DescribeUserScramCredentialsResult {
        error_code: ILLEGAL_SASL_STATE,
        error_message: Some(exception_message()),
        ..Default::default()
    };
    let altered = |user: &str| AlterUserScramCredentialsResult {
        user: user.to_owned(),
        error_code: ILLEGAL_SASL_STATE,
        error_message: Some(exception_message()),
        ..Default::default()
    };
    let deletion = |name: &str| ScramCredentialDeletion {
        name: name.to_owned(),
        ..Default::default()
    };
    let upsertion = |name: &str| ScramCredentialUpsertion {
        name: name.to_owned(),
        ..Default::default()
    };
    // U+1F600 sorts before U+FF21 in UTF-16, and after it in UTF-8.
    check(vec![
        case(
            ApiKey::DescribeUserScramCredentials,
            0,
            DescribeUserScramCredentialsRequest {
                users: Some(vec![UserName::default(), UserName::default()]),
                ..Default::default()
            },
            Some(DescribeUserScramCredentialsResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                results: vec![described.clone(), described],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeUserScramCredentials,
            0,
            DescribeUserScramCredentialsRequest {
                users: None,
                ..Default::default()
            },
            None::<DescribeUserScramCredentialsResponse>,
        ),
        case(
            ApiKey::AlterUserScramCredentials,
            0,
            AlterUserScramCredentialsRequest {
                deletions: vec![deletion("\u{1F600}"), deletion("bob")],
                upsertions: vec![upsertion("alice"), upsertion("bob"), upsertion("\u{FF21}")],
                ..Default::default()
            },
            Some(AlterUserScramCredentialsResponse {
                results: vec![
                    altered("alice"),
                    altered("bob"),
                    altered("\u{1F600}"),
                    altered("\u{FF21}"),
                ],
                ..Default::default()
            }),
        ),
    ]);
}

/// The delegation-token apis. `CreateDelegationToken` names an anonymous
/// requester only from v3.
#[test]
fn delegation_token_apis() {
    let created = |requester: &str| CreateDelegationTokenResponse {
        error_code: ILLEGAL_SASL_STATE,
        principal_type: "User".to_owned(),
        principal_name: "ANONYMOUS".to_owned(),
        token_requester_principal_type: if requester.is_empty() {
            String::new()
        } else {
            "User".to_owned()
        },
        token_requester_principal_name: requester.to_owned(),
        issue_timestamp_ms: -1,
        expiry_timestamp_ms: -1,
        max_timestamp_ms: -1,
        token_id: String::new(),
        hmac: Bytes::new(),
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::CreateDelegationToken,
            2,
            CreateDelegationTokenRequest::default(),
            Some(created("")),
        ),
        case(
            ApiKey::CreateDelegationToken,
            3,
            CreateDelegationTokenRequest::default(),
            Some(created("ANONYMOUS")),
        ),
        case(
            ApiKey::RenewDelegationToken,
            2,
            RenewDelegationTokenRequest::default(),
            Some(RenewDelegationTokenResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ExpireDelegationToken,
            2,
            ExpireDelegationTokenRequest::default(),
            Some(ExpireDelegationTokenResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeDelegationToken,
            3,
            DescribeDelegationTokenRequest::default(),
            Some(DescribeDelegationTokenResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}
