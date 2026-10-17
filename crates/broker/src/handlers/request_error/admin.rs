//! The topic, ACL, config, quota, credential, token and cluster admin apis.

use {
    super::{ErrorAnswer, Request, RequestFailure},
    bytes::Bytes,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            alter_client_quotas_request::AlterClientQuotasRequest,
            alter_client_quotas_response::{
                AlterClientQuotasResponse, EntityData as QuotaEntity, EntryData as QuotaEntry,
            },
            alter_configs_request::AlterConfigsRequest,
            alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
            alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
            alter_partition_reassignments_response::{
                AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
                ReassignableTopicResponse,
            },
            alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            alter_user_scram_credentials_response::{
                AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
            },
            create_acls_request::CreateAclsRequest,
            create_acls_response::{AclCreationResult, CreateAclsResponse},
            create_delegation_token_request::CreateDelegationTokenRequest,
            create_delegation_token_response::CreateDelegationTokenResponse,
            create_partitions_request::CreatePartitionsRequest,
            create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
            create_topics_request::CreateTopicsRequest,
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            delete_acls_request::DeleteAclsRequest,
            delete_acls_response::{DeleteAclsFilterResult, DeleteAclsResponse},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
            describe_acls_request::DescribeAclsRequest,
            describe_acls_response::DescribeAclsResponse,
            describe_client_quotas_request::DescribeClientQuotasRequest,
            describe_client_quotas_response::DescribeClientQuotasResponse,
            describe_cluster_request::DescribeClusterRequest,
            describe_cluster_response::DescribeClusterResponse,
            describe_configs_request::DescribeConfigsRequest,
            describe_configs_response::{DescribeConfigsResponse, DescribeConfigsResult},
            describe_delegation_token_request::DescribeDelegationTokenRequest,
            describe_delegation_token_response::DescribeDelegationTokenResponse,
            describe_log_dirs_request::DescribeLogDirsRequest,
            describe_log_dirs_response::DescribeLogDirsResponse,
            describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest,
            describe_user_scram_credentials_response::{
                DescribeUserScramCredentialsResponse, DescribeUserScramCredentialsResult,
            },
            elect_leaders_request::ElectLeadersRequest,
            elect_leaders_response::{
                ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
            },
            expire_delegation_token_request::ExpireDelegationTokenRequest,
            expire_delegation_token_response::ExpireDelegationTokenResponse,
            incremental_alter_configs_request::IncrementalAlterConfigsRequest,
            incremental_alter_configs_response::{
                AlterConfigsResourceResponse as IncrementalResourceResponse,
                IncrementalAlterConfigsResponse,
            },
            list_config_resources_request::ListConfigResourcesRequest,
            list_config_resources_response::ListConfigResourcesResponse,
            list_partition_reassignments_request::ListPartitionReassignmentsRequest,
            list_partition_reassignments_response::{
                ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
                OngoingTopicReassignment,
            },
            renew_delegation_token_request::RenewDelegationTokenRequest,
            renew_delegation_token_response::RenewDelegationTokenResponse,
            unregister_broker_request::UnregisterBrokerRequest,
            unregister_broker_response::UnregisterBrokerResponse,
            update_features_request::UpdateFeaturesRequest,
            update_features_response::UpdateFeaturesResponse,
        },
    },
};

/// The first `DeleteTopics` version that names topics by `DeleteTopicState`
/// rather than by name alone.
const DELETE_TOPICS_TOPIC_STATE_MIN_VERSION: i16 = 6;

/// The first `ElectLeaders` version with a top-level error code.
const ELECT_LEADERS_TOP_LEVEL_ERROR_MIN_VERSION: i16 = 1;

/// The first `CreateDelegationToken` version that carries the token
/// requester.
const CREATE_DELEGATION_TOKEN_REQUESTER_MIN_VERSION: i16 = 3;

/// `KafkaPrincipal.ANONYMOUS`, the owner and requester of the token that
/// `CreateDelegationTokenRequest.getErrorResponse` describes.
const ANONYMOUS_PRINCIPAL_TYPE: &str = "User";
const ANONYMOUS_PRINCIPAL_NAME: &str = "ANONYMOUS";

/// The `-1` timestamps of `CreateDelegationTokenResponse.prepareResponse`.
const NO_TIMESTAMP: i64 = -1;

pub(super) fn answer(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let failure = request.failure;
    let version = request.version;
    Some(match api {
        ApiKey::CreateTopics => request.respond(|req| create_topics(&req, failure)),
        ApiKey::DeleteTopics => request.respond(|req| delete_topics(req, version, failure)),
        ApiKey::CreatePartitions => request.respond(|req| create_partitions(&req, failure)),
        ApiKey::DescribeAcls => request.respond(|_: DescribeAclsRequest| DescribeAclsResponse {
            error_code: failure.error_code,
            error_message: failure.api_message(),
            ..Default::default()
        }),
        ApiKey::CreateAcls => request.respond(|req: CreateAclsRequest| CreateAclsResponse {
            results: vec![
                AclCreationResult {
                    error_code: failure.error_code,
                    error_message: failure.api_message(),
                    ..Default::default()
                };
                req.creations.len()
            ],
            ..Default::default()
        }),
        ApiKey::DeleteAcls => request.respond(|req: DeleteAclsRequest| DeleteAclsResponse {
            filter_results: vec![
                DeleteAclsFilterResult {
                    error_code: failure.error_code,
                    error_message: failure.api_message(),
                    ..Default::default()
                };
                req.filters.len()
            ],
            ..Default::default()
        }),
        ApiKey::ElectLeaders => request.respond(|req| elect_leaders(&req, version, failure)),
        ApiKey::AlterPartitionReassignments => {
            request.respond(|req| alter_partition_reassignments(&req, failure))
        }
        ApiKey::ListPartitionReassignments => {
            request.respond(|req| list_partition_reassignments(&req, failure))
        }
        ApiKey::DescribeCluster => {
            request.respond(|_: DescribeClusterRequest| DescribeClusterResponse {
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
        }
        ApiKey::UpdateFeatures => {
            request.respond(|_: UpdateFeaturesRequest| UpdateFeaturesResponse {
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
        }
        ApiKey::UnregisterBroker => {
            request.respond(|_: UnregisterBrokerRequest| UnregisterBrokerResponse {
                error_code: failure.error_code,
                error_message: Some(failure.exception_message()),
                ..Default::default()
            })
        }
        _ => return configs_or_credentials(api, request),
    })
}

/// The config, log-dir, quota, SCRAM-credential and delegation-token apis.
fn configs_or_credentials(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let failure = request.failure;
    let code = failure.error_code;
    Some(match api {
        ApiKey::DescribeConfigs => request.respond(|req| describe_configs(&req, failure)),
        ApiKey::AlterConfigs => request.respond(|req| alter_configs(&req, failure)),
        ApiKey::IncrementalAlterConfigs => {
            request.respond(|req| incremental_alter_configs(&req, failure))
        }
        ApiKey::ListConfigResources => {
            request.respond(
                |_: ListConfigResourcesRequest| ListConfigResourcesResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::AlterReplicaLogDirs => request.respond(|req| alter_replica_log_dirs(&req, code)),
        ApiKey::DescribeLogDirs => {
            request.respond(|_: DescribeLogDirsRequest| DescribeLogDirsResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::DescribeClientQuotas => {
            request.respond(
                |_: DescribeClientQuotasRequest| DescribeClientQuotasResponse {
                    error_code: code,
                    error_message: failure.api_message(),
                    entries: None,
                    ..Default::default()
                },
            )
        }
        ApiKey::AlterClientQuotas => request.respond(|req| alter_client_quotas(&req, failure)),
        ApiKey::DescribeUserScramCredentials => {
            request.maybe_respond(|req| describe_user_scram_credentials(&req, failure))
        }
        ApiKey::AlterUserScramCredentials => {
            request.respond(|req| alter_user_scram_credentials(&req, failure))
        }
        _ => return delegation_tokens(api, request),
    })
}

/// The four KIP-48 delegation-token apis.
fn delegation_tokens(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let version = request.version;
    Some(match api {
        ApiKey::CreateDelegationToken => request
            .respond(|_: CreateDelegationTokenRequest| create_delegation_token(version, code)),
        ApiKey::RenewDelegationToken => {
            request.respond(
                |_: RenewDelegationTokenRequest| RenewDelegationTokenResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::ExpireDelegationToken => {
            request.respond(
                |_: ExpireDelegationTokenRequest| ExpireDelegationTokenResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::DescribeDelegationToken => {
            request.respond(
                |_: DescribeDelegationTokenRequest| DescribeDelegationTokenResponse {
                    error_code: code,
                    tokens: Vec::new(),
                    ..Default::default()
                },
            )
        }
        _ => return None,
    })
}

/// `CreateTopicsRequest.getErrorResponse`: every requested topic with the
/// code and `ApiError`'s message.
fn create_topics(req: &CreateTopicsRequest, failure: &RequestFailure<'_>) -> CreateTopicsResponse {
    CreateTopicsResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| CreatableTopicResult {
                name: topic.name.clone(),
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DeleteTopicsRequest.getErrorResponse`: every requested topic with the
/// code, from `DeleteTopicsRequest.topics()`, which before v6 names each topic
/// by its name alone.
fn delete_topics(
    req: DeleteTopicsRequest,
    version: i16,
    failure: &RequestFailure<'_>,
) -> DeleteTopicsResponse {
    let topics = if version >= DELETE_TOPICS_TOPIC_STATE_MIN_VERSION {
        req.topics
    } else {
        req.topic_names
            .into_iter()
            .map(|name| DeleteTopicState {
                name: Some(name),
                ..Default::default()
            })
            .collect()
    };
    DeleteTopicsResponse {
        responses: topics
            .into_iter()
            .map(|topic| DeletableTopicResult {
                name: topic.name,
                topic_id: topic.topic_id,
                error_code: failure.error_code,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `CreatePartitionsRequest.getErrorResponse`: every requested topic with the
/// code and `ApiError`'s message.
fn create_partitions(
    req: &CreatePartitionsRequest,
    failure: &RequestFailure<'_>,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results: req
            .topics
            .iter()
            .map(|topic| CreatePartitionsTopicResult {
                name: topic.name.clone(),
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `ElectLeadersRequest.getErrorResponse`: every requested partition with the
/// code and `ApiError`'s message, and from v1 the top-level code.
fn elect_leaders(
    req: &ElectLeadersRequest,
    version: i16,
    failure: &RequestFailure<'_>,
) -> ElectLeadersResponse {
    ElectLeadersResponse {
        error_code: if version >= ELECT_LEADERS_TOP_LEVEL_ERROR_MIN_VERSION {
            failure.error_code
        } else {
            0
        },
        replica_election_results: rows!(req.topic_partitions.iter().flatten(), |topic| {
            ReplicaElectionResult {
                topic: topic.topic.clone(),
                partition_result: rows!(&topic.partitions, |&partition_id| PartitionResult {
                    partition_id,
                    error_code: failure.error_code,
                    error_message: failure.api_message(),
                }),
            }
        }),
        ..Default::default()
    }
}

/// `AlterPartitionReassignmentsRequest.getErrorResponse`: the code and
/// `ApiError`'s message at the top level and on every requested partition.
fn alter_partition_reassignments(
    req: &AlterPartitionReassignmentsRequest,
    failure: &RequestFailure<'_>,
) -> AlterPartitionReassignmentsResponse {
    AlterPartitionReassignmentsResponse {
        error_code: failure.error_code,
        error_message: failure.api_message(),
        responses: rows!(&req.topics, |topic| ReassignableTopicResponse {
            name: topic.name.clone(),
            partitions: rows!(&topic.partitions, |partition| {
                ReassignablePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: failure.error_code,
                    error_message: failure.api_message(),
                }
            }),
        }),
        ..Default::default()
    }
}

/// `ListPartitionReassignmentsRequest.getErrorResponse`: the code and
/// `ApiError`'s message, and every requested partition with no replicas.
fn list_partition_reassignments(
    req: &ListPartitionReassignmentsRequest,
    failure: &RequestFailure<'_>,
) -> ListPartitionReassignmentsResponse {
    ListPartitionReassignmentsResponse {
        error_code: failure.error_code,
        error_message: failure.api_message(),
        topics: req
            .topics
            .iter()
            .flatten()
            .map(|topic| OngoingTopicReassignment {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|&partition_index| OngoingPartitionReassignment {
                        partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DescribeConfigsRequest.getErrorResponse`: every requested resource with
/// the code and the code's default message.
fn describe_configs(
    req: &DescribeConfigsRequest,
    failure: &RequestFailure<'_>,
) -> DescribeConfigsResponse {
    DescribeConfigsResponse {
        results: rows!(&req.resources, |resource| DescribeConfigsResult {
            error_code: failure.error_code,
            error_message: Some(failure.error_message()),
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
        }),
        ..Default::default()
    }
}

/// `AlterConfigsRequest.getErrorResponse`: every requested resource with the
/// code and `ApiError`'s message.
fn alter_configs(req: &AlterConfigsRequest, failure: &RequestFailure<'_>) -> AlterConfigsResponse {
    AlterConfigsResponse {
        responses: rows!(&req.resources, |resource| AlterConfigsResourceResponse {
            error_code: failure.error_code,
            error_message: failure.api_message(),
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
        }),
        ..Default::default()
    }
}

/// `IncrementalAlterConfigsRequest.getErrorResponse`: every requested
/// resource with the code and `ApiError`'s message.
fn incremental_alter_configs(
    req: &IncrementalAlterConfigsRequest,
    failure: &RequestFailure<'_>,
) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse {
        responses: rows!(&req.resources, |resource| IncrementalResourceResponse {
            error_code: failure.error_code,
            error_message: failure.api_message(),
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
        }),
        ..Default::default()
    }
}

/// `AlterReplicaLogDirsRequest.getErrorResponse`: one result per directory and
/// topic pair, in request order, with every partition carrying the code.
fn alter_replica_log_dirs(
    req: &AlterReplicaLogDirsRequest,
    code: i16,
) -> AlterReplicaLogDirsResponse {
    AlterReplicaLogDirsResponse {
        results: req
            .dirs
            .iter()
            .flat_map(|dir| &dir.topics)
            .map(|topic| AlterReplicaLogDirTopicResult {
                topic_name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|&partition_index| AlterReplicaLogDirPartitionResult {
                        partition_index,
                        error_code: code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `AlterClientQuotasRequest.getErrorResponse`: every requested entry with its
/// entity, the code and the code's default message.
fn alter_client_quotas(
    req: &AlterClientQuotasRequest,
    failure: &RequestFailure<'_>,
) -> AlterClientQuotasResponse {
    AlterClientQuotasResponse {
        entries: req
            .entries
            .iter()
            .map(|entry| QuotaEntry {
                error_code: failure.error_code,
                error_message: Some(failure.error_message()),
                entity: entry
                    .entity
                    .iter()
                    .map(|entity| QuotaEntity {
                        entity_type: entity.entity_type.clone(),
                        entity_name: entity.entity_name.clone(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DescribeUserScramCredentialsRequest.getErrorResponse`: the code and
/// `ApiError`'s message at the top level and once per requested user. A null
/// user list throws there, so it gets no response.
fn describe_user_scram_credentials(
    req: &DescribeUserScramCredentialsRequest,
    failure: &RequestFailure<'_>,
) -> Option<DescribeUserScramCredentialsResponse> {
    let users = req.users.as_ref()?;
    Some(DescribeUserScramCredentialsResponse {
        error_code: failure.error_code,
        error_message: failure.api_message(),
        results: users
            .iter()
            .map(|_| DescribeUserScramCredentialsResult {
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// `AlterUserScramCredentialsRequest.getErrorResponse`: one result per
/// distinct user named by a deletion or an upsertion, sorted as Java sorts
/// strings, each with the code and `ApiError`'s message.
fn alter_user_scram_credentials(
    req: &AlterUserScramCredentialsRequest,
    failure: &RequestFailure<'_>,
) -> AlterUserScramCredentialsResponse {
    let mut users: Vec<&str> = req
        .deletions
        .iter()
        .map(|deletion| deletion.name.as_str())
        .chain(
            req.upsertions
                .iter()
                .map(|upsertion| upsertion.name.as_str()),
        )
        .collect();
    // `String.compareTo` orders by UTF-16 code unit.
    users.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    users.dedup();
    AlterUserScramCredentialsResponse {
        results: users
            .into_iter()
            .map(|user| AlterUserScramCredentialsResult {
                user: user.to_owned(),
                error_code: failure.error_code,
                error_message: failure.api_message(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `CreateDelegationTokenResponse.prepareResponse` for the error: an anonymous
/// owner, from v3 an anonymous requester, `-1` timestamps, and no token.
fn create_delegation_token(version: i16, code: i16) -> CreateDelegationTokenResponse {
    let (token_requester_principal_type, token_requester_principal_name) =
        if version >= CREATE_DELEGATION_TOKEN_REQUESTER_MIN_VERSION {
            (
                ANONYMOUS_PRINCIPAL_TYPE.to_owned(),
                ANONYMOUS_PRINCIPAL_NAME.to_owned(),
            )
        } else {
            (String::new(), String::new())
        };
    CreateDelegationTokenResponse {
        error_code: code,
        principal_type: ANONYMOUS_PRINCIPAL_TYPE.to_owned(),
        principal_name: ANONYMOUS_PRINCIPAL_NAME.to_owned(),
        token_requester_principal_type,
        token_requester_principal_name,
        issue_timestamp_ms: NO_TIMESTAMP,
        expiry_timestamp_ms: NO_TIMESTAMP,
        max_timestamp_ms: NO_TIMESTAMP,
        token_id: String::new(),
        hmac: Bytes::new(),
        ..Default::default()
    }
}
