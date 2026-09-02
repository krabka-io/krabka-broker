//! Registration tables for the apis whose handler takes a [`RequestContext`],
//! both the async ones and the synchronous ones whose adapter wraps the result
//! in an already-ready future.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::api_key::ApiKey;

use super::{DispatchEntry, DispatchRegistry};
use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext},
};

context_dispatches!(register_context_dispatches;
    (metadata_adapter, Metadata, metadata_request, crate::handlers::metadata::handle),
    (describe_cluster_adapter, DescribeCluster, describe_cluster_request, crate::handlers::describe_cluster::handle),
    (describe_topic_partitions_adapter, DescribeTopicPartitions, describe_topic_partitions_request, crate::handlers::describe_topic_partitions::handle),
    (create_topics_adapter, CreateTopics, create_topics_request, crate::handlers::create_topics::handle),
    (delete_topics_adapter, DeleteTopics, delete_topics_request, crate::handlers::delete_topics::handle),
    (alter_configs_adapter, AlterConfigs, alter_configs_request, crate::handlers::alter_configs::handle),
    (incremental_alter_configs_adapter, IncrementalAlterConfigs, incremental_alter_configs_request, crate::handlers::incremental_alter_configs::handle),
    (delete_records_adapter, DeleteRecords, delete_records_request, crate::handlers::delete_records::handle),
    (create_partitions_adapter, CreatePartitions, create_partitions_request, crate::handlers::create_partitions::handle),
    (describe_groups_adapter, DescribeGroups, describe_groups_request, crate::handlers::describe_groups::handle),
    (list_groups_adapter, ListGroups, list_groups_request, crate::handlers::list_groups::handle),
    (share_group_describe_adapter, ShareGroupDescribe, share_group_describe_request, crate::handlers::share_group_describe::handle),
    (share_fetch_adapter, ShareFetch, share_fetch_request, crate::handlers::share_fetch::handle),
    (share_acknowledge_adapter, ShareAcknowledge, share_acknowledge_request, crate::handlers::share_acknowledge::handle),
    (describe_share_group_offsets_adapter, DescribeShareGroupOffsets, describe_share_group_offsets_request, crate::handlers::describe_share_group_offsets::handle),
    (alter_share_group_offsets_adapter, AlterShareGroupOffsets, alter_share_group_offsets_request, crate::handlers::alter_share_group_offsets::handle),
    (delete_share_group_offsets_adapter, DeleteShareGroupOffsets, delete_share_group_offsets_request, crate::handlers::delete_share_group_offsets::handle),
    (delete_groups_adapter, DeleteGroups, delete_groups_request, crate::handlers::delete_groups::handle),
    (join_group_adapter, JoinGroup, join_group_request, crate::handlers::join_group::handle),
    (offset_commit_adapter, OffsetCommit, offset_commit_request, crate::handlers::offset_commit::handle),
    (offset_fetch_adapter, OffsetFetch, offset_fetch_request, crate::handlers::offset_fetch::handle),
    (offset_delete_adapter, OffsetDelete, offset_delete_request, crate::handlers::offset_delete::handle),
    (describe_producers_adapter, DescribeProducers, describe_producers_request, crate::handlers::describe_producers::handle),
    (describe_transactions_adapter, DescribeTransactions, describe_transactions_request, crate::handlers::describe_transactions::handle),
    (list_transactions_adapter, ListTransactions, list_transactions_request, crate::handlers::list_transactions::handle),
    (unregister_broker_adapter, UnregisterBroker, unregister_broker_request, crate::handlers::unregister_broker::handle),
    (add_raft_voter_adapter, AddRaftVoter, add_raft_voter_request, crate::handlers::add_raft_voter::handle),
    (remove_raft_voter_adapter, RemoveRaftVoter, remove_raft_voter_request, crate::handlers::remove_raft_voter::handle),
    (update_raft_voter_adapter, UpdateRaftVoter, update_raft_voter_request, crate::handlers::update_raft_voter::handle),
    (alter_partition_adapter, AlterPartition, alter_partition_request, crate::handlers::alter_partition::handle),
    (broker_heartbeat_adapter, BrokerHeartbeat, broker_heartbeat_request, crate::handlers::broker_heartbeat::handle),
    (broker_registration_adapter, BrokerRegistration, broker_registration_request, crate::handlers::broker_registration::handle),
    (controller_registration_adapter, ControllerRegistration, controller_registration_request, crate::handlers::controller_registration::handle),
    (heartbeat_adapter, Heartbeat, heartbeat_request, crate::handlers::heartbeat::handle),
    (sync_group_adapter, SyncGroup, sync_group_request, crate::handlers::sync_group::handle),
    (leave_group_adapter, LeaveGroup, leave_group_request, crate::handlers::leave_group::handle),
    (consumer_group_heartbeat_adapter, ConsumerGroupHeartbeat, consumer_group_heartbeat_request, crate::handlers::consumer_group_heartbeat::handle),
    (share_group_heartbeat_adapter, ShareGroupHeartbeat, share_group_heartbeat_request, crate::handlers::share_group_heartbeat::handle),
    (streams_group_heartbeat_adapter, StreamsGroupHeartbeat, streams_group_heartbeat_request, crate::handlers::streams_group_heartbeat::handle),
    (consumer_group_describe_adapter, ConsumerGroupDescribe, consumer_group_describe_request, crate::handlers::consumer_group_describe::handle),
    (streams_group_describe_adapter, StreamsGroupDescribe, streams_group_describe_request, crate::handlers::streams_group_describe::handle),
    (find_coordinator_adapter, FindCoordinator, find_coordinator_request, crate::handlers::find_coordinator::handle),
    (list_offsets_adapter, ListOffsets, list_offsets_request, crate::handlers::list_offsets::handle),
    (describe_log_dirs_adapter, DescribeLogDirs, describe_log_dirs_request, crate::handlers::describe_log_dirs::handle),
    (init_producer_id_adapter, InitProducerId, init_producer_id_request, crate::handlers::init_producer_id::handle),
    (add_partitions_to_txn_adapter, AddPartitionsToTxn, add_partitions_to_txn_request, crate::txn::handlers::add_partitions_to_txn::handle),
    (end_txn_adapter, EndTxn, end_txn_request, crate::txn::handlers::end_txn::handle),
    (txn_offset_commit_adapter, TxnOffsetCommit, txn_offset_commit_request, crate::txn::handlers::txn_offset_commit::handle),
);

sync_context_dispatches!(register_sync_context_dispatches;
    (list_config_resources_adapter, ListConfigResources, list_config_resources_request, crate::handlers::list_config_resources::handle),
    (describe_quorum_adapter, DescribeQuorum, describe_quorum_request, crate::handlers::describe_quorum::handle),
    (get_replica_log_info_adapter, GetReplicaLogInfo, get_replica_log_info_request, crate::handlers::get_replica_log_info::handle),
    (offset_for_leader_epoch_adapter, OffsetForLeaderEpoch, offset_for_leader_epoch_request, crate::handlers::offset_for_leader_epoch::handle),
    (describe_configs_adapter, DescribeConfigs, describe_configs_request, crate::handlers::describe_configs::handle),
);
