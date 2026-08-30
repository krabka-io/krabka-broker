//! Typed error responses for requests outside an API's supported version range.
//!
//! The generated protocol crate does not expose Kafka's getErrorResponse
//! equivalent. Keep the dispatch rejection before request decoding, but encode
//! a real response at the nearest schema version so clients receive error 35
//! instead of an unexplained disconnect.

use bytes::Bytes;

use crate::error::BrokerError;

macro_rules! encoded {
    ($module:ident, $response:expr, $version:expr) => {{
        use krabka_protocol::owned::$module::*;
        crate::handlers::encode_response(&$response, $version)
    }};
}

/// Encode a schema-valid response carrying `UNSUPPORTED_VERSION`.
///
/// Returns `None` for API keys without a response shape in this table.
#[allow(clippy::too_many_lines)]
pub(super) fn body(api_key: i16, version: i16) -> Option<Result<Bytes, BrokerError>> {
    match api_key {
        0 => Some(encoded!(
            produce_response,
            ProduceResponse {
                responses: vec![TopicProduceResponse {
                    partition_responses: vec![PartitionProduceResponse {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        1 => Some(encoded!(
            fetch_response,
            FetchResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![FetchableTopicResponse {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        2 => Some(encoded!(
            list_offsets_response,
            ListOffsetsResponse {
                topics: vec![ListOffsetsTopicResponse {
                    partitions: vec![ListOffsetsPartitionResponse {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        3 => Some(encoded!(
            metadata_response,
            MetadataResponse {
                topics: vec![MetadataResponseTopic {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    partitions: vec![MetadataResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        8 => Some(encoded!(
            offset_commit_response,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    partitions: vec![OffsetCommitResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        9 => Some(encoded!(
            offset_fetch_response,
            OffsetFetchResponse {
                topics: vec![OffsetFetchResponseTopic {
                    partitions: vec![OffsetFetchResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                error_code: crate::codes::UNSUPPORTED_VERSION,
                groups: vec![OffsetFetchResponseGroup {
                    topics: vec![OffsetFetchResponseTopics {
                        partitions: vec![OffsetFetchResponsePartitions {
                            error_code: crate::codes::UNSUPPORTED_VERSION,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        10 => Some(encoded!(
            find_coordinator_response,
            FindCoordinatorResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                coordinators: vec![Coordinator {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        11 => Some(encoded!(
            join_group_response,
            JoinGroupResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        12 => Some(encoded!(
            heartbeat_response,
            HeartbeatResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        13 => Some(encoded!(
            leave_group_response,
            LeaveGroupResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                members: vec![MemberResponse {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        14 => Some(encoded!(
            sync_group_response,
            SyncGroupResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        15 => Some(encoded!(
            describe_groups_response,
            DescribeGroupsResponse {
                groups: vec![DescribedGroup {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        16 => Some(encoded!(
            list_groups_response,
            ListGroupsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        17 => Some(encoded!(
            sasl_handshake_response,
            SaslHandshakeResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        18 => Some(encoded!(
            api_versions_response,
            ApiVersionsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        19 => Some(encoded!(
            create_topics_response,
            CreateTopicsResponse {
                topics: vec![CreatableTopicResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        20 => Some(encoded!(
            delete_topics_response,
            DeleteTopicsResponse {
                responses: vec![DeletableTopicResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        21 => Some(encoded!(
            delete_records_response,
            DeleteRecordsResponse {
                topics: vec![DeleteRecordsTopicResult {
                    partitions: vec![DeleteRecordsPartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        22 => Some(encoded!(
            init_producer_id_response,
            InitProducerIdResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        23 => Some(encoded!(
            offset_for_leader_epoch_response,
            OffsetForLeaderEpochResponse {
                topics: vec![OffsetForLeaderTopicResult {
                    partitions: vec![EpochEndOffset {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        24 => Some(encoded!(
            add_partitions_to_txn_response,
            AddPartitionsToTxnResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        25 => Some(encoded!(
            add_offsets_to_txn_response,
            AddOffsetsToTxnResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        26 => Some(encoded!(
            end_txn_response,
            EndTxnResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        27 => Some(encoded!(
            write_txn_markers_response,
            WriteTxnMarkersResponse {
                markers: vec![WritableTxnMarkerResult {
                    topics: vec![WritableTxnMarkerTopicResult {
                        partitions: vec![WritableTxnMarkerPartitionResult {
                            error_code: crate::codes::UNSUPPORTED_VERSION,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        28 => Some(encoded!(
            txn_offset_commit_response,
            TxnOffsetCommitResponse {
                topics: vec![TxnOffsetCommitResponseTopic {
                    partitions: vec![TxnOffsetCommitResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        29 => Some(encoded!(
            describe_acls_response,
            DescribeAclsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        30 => Some(encoded!(
            create_acls_response,
            CreateAclsResponse {
                results: vec![AclCreationResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        31 => Some(encoded!(
            delete_acls_response,
            DeleteAclsResponse {
                filter_results: vec![DeleteAclsFilterResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    matching_acls: vec![DeleteAclsMatchingAcl {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        32 => Some(encoded!(
            describe_configs_response,
            DescribeConfigsResponse {
                results: vec![DescribeConfigsResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        33 => Some(encoded!(
            alter_configs_response,
            AlterConfigsResponse {
                responses: vec![AlterConfigsResourceResponse {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        34 => Some(encoded!(
            alter_replica_log_dirs_response,
            AlterReplicaLogDirsResponse {
                results: vec![AlterReplicaLogDirTopicResult {
                    partitions: vec![AlterReplicaLogDirPartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        35 => Some(encoded!(
            describe_log_dirs_response,
            DescribeLogDirsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                results: vec![DescribeLogDirsResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        36 => Some(encoded!(
            sasl_authenticate_response,
            SaslAuthenticateResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        37 => Some(encoded!(
            create_partitions_response,
            CreatePartitionsResponse {
                results: vec![CreatePartitionsTopicResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        38 => Some(encoded!(
            create_delegation_token_response,
            CreateDelegationTokenResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        39 => Some(encoded!(
            renew_delegation_token_response,
            RenewDelegationTokenResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        40 => Some(encoded!(
            expire_delegation_token_response,
            ExpireDelegationTokenResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        41 => Some(encoded!(
            describe_delegation_token_response,
            DescribeDelegationTokenResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        42 => Some(encoded!(
            delete_groups_response,
            DeleteGroupsResponse {
                results: vec![DeletableGroupResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        43 => Some(encoded!(
            elect_leaders_response,
            ElectLeadersResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                replica_election_results: vec![ReplicaElectionResult {
                    partition_result: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        44 => Some(encoded!(
            incremental_alter_configs_response,
            IncrementalAlterConfigsResponse {
                responses: vec![AlterConfigsResourceResponse {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        45 => Some(encoded!(
            alter_partition_reassignments_response,
            AlterPartitionReassignmentsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![ReassignableTopicResponse {
                    partitions: vec![ReassignablePartitionResponse {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        46 => Some(encoded!(
            list_partition_reassignments_response,
            ListPartitionReassignmentsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        47 => Some(encoded!(
            offset_delete_response,
            OffsetDeleteResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![OffsetDeleteResponseTopic {
                    partitions: vec![OffsetDeleteResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        48 => Some(encoded!(
            describe_client_quotas_response,
            DescribeClientQuotasResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        49 => Some(encoded!(
            alter_client_quotas_response,
            AlterClientQuotasResponse {
                entries: vec![EntryData {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        50 => Some(encoded!(
            describe_user_scram_credentials_response,
            DescribeUserScramCredentialsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                results: vec![DescribeUserScramCredentialsResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        51 => Some(encoded!(
            alter_user_scram_credentials_response,
            AlterUserScramCredentialsResponse {
                results: vec![AlterUserScramCredentialsResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        52 => Some(encoded!(
            vote_response,
            VoteResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicData {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        53 => Some(encoded!(
            begin_quorum_epoch_response,
            BeginQuorumEpochResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicData {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        54 => Some(encoded!(
            end_quorum_epoch_response,
            EndQuorumEpochResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicData {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        55 => Some(encoded!(
            describe_quorum_response,
            DescribeQuorumResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicData {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        56 => Some(encoded!(
            alter_partition_response,
            AlterPartitionResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicData {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        57 => Some(encoded!(
            update_features_response,
            UpdateFeaturesResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                results: vec![UpdatableFeatureResult {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        58 => Some(encoded!(
            envelope_response,
            EnvelopeResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        59 => Some(encoded!(
            fetch_snapshot_response,
            FetchSnapshotResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                topics: vec![TopicSnapshot {
                    partitions: vec![PartitionSnapshot {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        60 => Some(encoded!(
            describe_cluster_response,
            DescribeClusterResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        61 => Some(encoded!(
            describe_producers_response,
            DescribeProducersResponse {
                topics: vec![TopicResponse {
                    partitions: vec![PartitionResponse {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        62 => Some(encoded!(
            broker_registration_response,
            BrokerRegistrationResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        63 => Some(encoded!(
            broker_heartbeat_response,
            BrokerHeartbeatResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        64 => Some(encoded!(
            unregister_broker_response,
            UnregisterBrokerResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        65 => Some(encoded!(
            describe_transactions_response,
            DescribeTransactionsResponse {
                transaction_states: vec![TransactionState {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        66 => Some(encoded!(
            list_transactions_response,
            ListTransactionsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        67 => Some(encoded!(
            allocate_producer_ids_response,
            AllocateProducerIdsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        68 => Some(encoded!(
            consumer_group_heartbeat_response,
            ConsumerGroupHeartbeatResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        69 => Some(encoded!(
            consumer_group_describe_response,
            ConsumerGroupDescribeResponse {
                groups: vec![DescribedGroup {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        70 => Some(encoded!(
            controller_registration_response,
            ControllerRegistrationResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        71 => Some(encoded!(
            get_telemetry_subscriptions_response,
            GetTelemetrySubscriptionsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        72 => Some(encoded!(
            push_telemetry_response,
            PushTelemetryResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        73 => Some(encoded!(
            assign_replicas_to_dirs_response,
            AssignReplicasToDirsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                directories: vec![DirectoryData {
                    topics: vec![TopicData {
                        partitions: vec![PartitionData {
                            error_code: crate::codes::UNSUPPORTED_VERSION,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        74 => Some(encoded!(
            list_config_resources_response,
            ListConfigResourcesResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        75 => Some(encoded!(
            describe_topic_partitions_response,
            DescribeTopicPartitionsResponse {
                topics: vec![DescribeTopicPartitionsResponseTopic {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    partitions: vec![DescribeTopicPartitionsResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        76 => Some(encoded!(
            share_group_heartbeat_response,
            ShareGroupHeartbeatResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        77 => Some(encoded!(
            share_group_describe_response,
            ShareGroupDescribeResponse {
                groups: vec![DescribedGroup {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        78 => Some(encoded!(
            share_fetch_response,
            ShareFetchResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![ShareFetchableTopicResponse {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        79 => Some(encoded!(
            share_acknowledge_response,
            ShareAcknowledgeResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![ShareAcknowledgeTopicResponse {
                    partitions: vec![PartitionData {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        80 => Some(encoded!(
            add_raft_voter_response,
            AddRaftVoterResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        81 => Some(encoded!(
            remove_raft_voter_response,
            RemoveRaftVoterResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        82 => Some(encoded!(
            update_raft_voter_response,
            UpdateRaftVoterResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        83 => Some(encoded!(
            initialize_share_group_state_response,
            InitializeShareGroupStateResponse {
                results: vec![InitializeStateResult {
                    partitions: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        84 => Some(encoded!(
            read_share_group_state_response,
            ReadShareGroupStateResponse {
                results: vec![ReadStateResult {
                    partitions: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        85 => Some(encoded!(
            write_share_group_state_response,
            WriteShareGroupStateResponse {
                results: vec![WriteStateResult {
                    partitions: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        86 => Some(encoded!(
            delete_share_group_state_response,
            DeleteShareGroupStateResponse {
                results: vec![DeleteStateResult {
                    partitions: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        87 => Some(encoded!(
            read_share_group_state_summary_response,
            ReadShareGroupStateSummaryResponse {
                results: vec![ReadStateSummaryResult {
                    partitions: vec![PartitionResult {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        88 => Some(encoded!(
            streams_group_heartbeat_response,
            StreamsGroupHeartbeatResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
            version
        )),
        89 => Some(encoded!(
            streams_group_describe_response,
            StreamsGroupDescribeResponse {
                groups: vec![DescribedGroup {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        90 => Some(encoded!(
            describe_share_group_offsets_response,
            DescribeShareGroupOffsetsResponse {
                groups: vec![DescribeShareGroupOffsetsResponseGroup {
                    topics: vec![DescribeShareGroupOffsetsResponseTopic {
                        partitions: vec![DescribeShareGroupOffsetsResponsePartition {
                            error_code: crate::codes::UNSUPPORTED_VERSION,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        91 => Some(encoded!(
            alter_share_group_offsets_response,
            AlterShareGroupOffsetsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![AlterShareGroupOffsetsResponseTopic {
                    partitions: vec![AlterShareGroupOffsetsResponsePartition {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        92 => Some(encoded!(
            delete_share_group_offsets_response,
            DeleteShareGroupOffsetsResponse {
                error_code: crate::codes::UNSUPPORTED_VERSION,
                responses: vec![DeleteShareGroupOffsetsResponseTopic {
                    error_code: crate::codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        93 => Some(encoded!(
            get_replica_log_info_response,
            GetReplicaLogInfoResponse {
                topic_partition_log_info_list: vec![TopicPartitionLogInfo {
                    partition_log_info: vec![PartitionLogInfo {
                        error_code: crate::codes::UNSUPPORTED_VERSION,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            version
        )),
        _ => None,
    }
}
