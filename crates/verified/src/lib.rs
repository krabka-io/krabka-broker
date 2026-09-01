//! Formally verified pure kernels shared by Krabka crates.
//!
//! Every function here is a total, synchronous, allocation-light kernel, and
//! Creusot proves its functional contract. See `docs/verification.md`. Host
//! crates call through to these functions, and there are no duplicate bodies
//! anywhere.
#![doc(html_root_url = "https://docs.rs/krabka-verified/0.5.2")]

pub mod audit;
pub mod authz;
pub mod break_glass;
pub mod broker;
pub mod chain;
pub mod checkpoint;
pub mod compaction;
pub mod consensus;
pub mod delegation_token;
pub mod delivery;
pub mod directory;
pub mod diskless;
pub mod epoch;
pub mod features;
pub mod isr;
pub mod leader_epoch;
pub mod local_recovery;
pub mod log_index;
pub mod offset_allocator;
pub mod produce;
pub mod producer;
pub mod producer_snapshot;
pub mod quorum_state;
pub mod raft;
pub mod reassignment;
pub mod reconfiguration;
pub mod recovery;
pub mod registration;
pub mod remote_metadata;
pub mod remote_read;
pub mod remote_txn;
pub mod restore;
pub mod retention;
pub mod schema;
pub mod share;
pub mod snapshot;
pub mod stamp;
pub mod storage;
pub mod stretch;
pub mod throttle;
pub mod timestamp;
pub mod transaction;
pub mod vote;
pub mod voter_set;
pub mod wal;

pub use audit::{SpoolAppendDecision, spool_append_decision};
pub use authz::{
    AclDecision, AclOperationKind, AclPatternKind, acl_decision, acl_identity_match,
    acl_operation_match, acl_resource_match,
};
pub use break_glass::{BreakGlassAdmission, break_glass_admission, select_break_glass_candidate};
pub use broker::{
    FetchVisibility, FetchWatermarks, ReplicaFetchMutation, delete_records_offset_out_of_range,
    delete_records_target, effective_share_backlog, fetch_visibility,
    preferred_rebalance_admission, replica_fetch_mutation, unclean_recovery_commit_admission,
};
pub use chain::{ChainStep, chain_step, select_chain_tip};
pub use checkpoint::{checkpoint_id_newer, checkpoint_id_retained, latest_checkpoint_index};
pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{
    election_has_quorum, election_jitter_ms, handoff_high_watermark, log_is_up_to_date,
    majority_size, recompute_high_watermark,
};
pub use delegation_token::{
    TokenCreateDecision, TokenDeadlines, TokenExpireDecision, TokenRenewDecision,
    create_token_deadlines, expire_token_deadline, renew_token_expiry, token_is_active,
};
pub use delivery::{
    coalesce_delivery_range, delivery_watermark_advance, scheduled_delivery_visible,
};
pub use directory::{
    DirectoryAssignmentDecision, DirectoryResponseDecision, directory_assignment_decision,
    directory_response_decision,
};
pub use diskless::{
    DisklessBatchStep, DisklessTrimDecision, diskless_batch_step, diskless_logical_range,
    diskless_span_extension, diskless_trim_decision,
};
pub use epoch::exact_epoch_successor;
pub use features::{FeatureUpdateDecision, feature_update_decision};
pub use leader_epoch::{EpochEntry, epoch_and_offset_for_entries};
pub use local_recovery::{
    LocalRecoveryStep, LocalRecoverySwapAction, local_recovery_batch_step,
    local_recovery_index_frontier, local_recovery_sealed_last, local_recovery_segment_chain,
    local_recovery_swap_action,
};
pub use log_index::{offset_index_lookup, offset_index_position_at_or_after, time_index_lookup};
pub use offset_allocator::reserve_offsets;
pub use produce::{ProduceBatchAdmission, produce_batch_admission, produce_durability_frontier};
pub use producer::{
    ProducerBatch, ProducerDecision, decrement_sequence, increment_sequence, producer_decision,
};
pub use producer_snapshot::{
    producer_snapshot_entry_valid, producer_snapshot_latest_index, producer_snapshot_replay_start,
    producer_snapshot_retained,
};
pub use quorum_state::{
    QuorumStateLoadDecision, QuorumStateWriteDecision, quorum_state_load_decision,
    quorum_state_write_decision,
};
pub use raft::{FetchResponseMutation, fetch_response_mutation, metadata_record_coordinates};
pub use reassignment::{
    ReassignmentAction, ReassignmentSetMembership, reassignment_action,
    reassignment_plan_admission, reassignment_set_membership,
};
pub use reconfiguration::{
    VoterChangeKind, VoterReconfigurationDecision, VoterReconfigurationPlan,
    voter_reconfiguration_decision,
};
pub use recovery::{
    ReplayCursorDecision, ReplayRecordDecision, replay_batch_cursor_decision,
    replay_cursor_decision, replay_record_decision, should_capture_first_downgrade,
};
pub use registration::{
    BrokerHeartbeatDecision, BrokerRegistrationDecision, broker_heartbeat_decision,
    broker_registration_decision,
};
pub use remote_metadata::remote_metadata_partition;
pub use remote_read::{
    remote_read_relative_offset, remote_time_index_candidate_count, remote_time_index_offset_usable,
};
pub use remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision};
pub use restore::{
    RESTORE_SNAPSHOT_DELETE_FINISHED, RESTORE_SNAPSHOT_DELETE_STARTED, RESTORE_SNAPSHOT_LIVE,
    RESTORE_SNAPSHOT_MISSING, restore_archive_reconcile, restore_batch_step,
    restore_record_coordinates,
};
pub use retention::{
    RetentionPrefix, barrier_cut_expired, local_retention_prefix, retention_delete_target,
    retention_prefix,
};
pub use schema::{SchemaFailureDecision, SchemaFailureKind, schema_failure_decision};
pub use share::share_prune_frontier;
pub use snapshot::{
    SnapshotChunkDecision, SnapshotInstallDecision, snapshot_chunk_admission,
    snapshot_install_decision, snapshot_prune_admission,
};
pub use stamp::{
    covering_stamp_range_index, exact_stamp_range_index, stamp_range_insertion_index,
    stamp_ranges_valid,
};
pub use storage::{
    LocalTruncationPlan, future_log_swap_admission, local_append_coordinates,
    local_truncation_plan, remote_partition_delete_transition, remote_segment_transition,
    truncation_batch_retained, truncation_frontier, truncation_relative_offset,
};
pub use stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
pub use timestamp::{
    earliest_max_timestamp_index, first_timestamp_index, timestamp_record_coordinates,
    timestamp_scan_next, timestamp_scan_window,
};
pub use transaction::{
    IdleTransactionState, NO_TRANSACTION_TIMEOUT_MS, TransactionCompletionDecision,
    TransactionIdentity, TransactionSnapshot, aborted_transaction_interval,
    aborted_transaction_overlaps, first_unstable_offset, resolve_transaction_timeout,
    should_abort_idle_transaction, transaction_completion_decision, transaction_marker_closes,
};
pub use vote::{
    VoteAdmissionDecision, VoteEncodeDecision, VoteWireDecision, vote_admission_decision,
    vote_encode_decision, vote_wire_decision,
};
pub use voter_set::{
    VoterSetWireDecision, VoterWireDecision, voter_set_wire_decision, voter_wire_decision,
};
pub use wal::{
    WalFetchAdmission, exact_wal_batch_range, select_wal_voter_index, wal_fetch_admission,
};
