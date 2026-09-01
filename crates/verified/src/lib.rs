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
pub mod compaction;
pub mod consensus;
pub mod delegation_token;
pub mod delivery;
pub mod diskless;
pub mod isr;
pub mod leader_epoch;
pub mod log_index;
pub mod offset_allocator;
pub mod produce;
pub mod producer;
pub mod raft;
pub mod reassignment;
pub mod reconfiguration;
pub mod recovery;
pub mod remote_metadata;
pub mod remote_read;
pub mod remote_txn;
pub mod restore;
pub mod retention;
pub mod schema;
pub mod storage;
pub mod stretch;
pub mod throttle;
pub mod transaction;
pub mod vote;
pub mod wal;

pub use audit::{SpoolAppendDecision, spool_append_decision};
pub use authz::{AclDecision, acl_decision};
pub use break_glass::{BreakGlassAdmission, break_glass_admission, select_break_glass_candidate};
pub use broker::{
    FetchVisibility, FetchWatermarks, delete_records_offset_out_of_range, delete_records_target,
    effective_share_backlog, fetch_visibility, unclean_recovery_commit_admission,
};
pub use chain::{ChainStep, chain_step, select_chain_tip};
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
pub use diskless::{DisklessTrimDecision, diskless_trim_decision};
pub use leader_epoch::{EpochEntry, epoch_and_offset_for_entries};
pub use log_index::{offset_index_lookup, offset_index_position_at_or_after, time_index_lookup};
pub use offset_allocator::reserve_offsets;
pub use produce::{ProduceBatchAdmission, produce_batch_admission};
pub use producer::{
    ProducerBatch, ProducerDecision, decrement_sequence, increment_sequence, producer_decision,
};
pub use raft::{FetchResponseMutation, fetch_response_mutation, metadata_record_coordinates};
pub use reassignment::{ReassignmentAction, reassignment_action};
pub use reconfiguration::{
    VoterChangeKind, VoterReconfigurationDecision, VoterReconfigurationPlan,
    voter_reconfiguration_decision,
};
pub use recovery::{
    ReplayCursorDecision, ReplayRecordDecision, replay_batch_cursor_decision,
    replay_cursor_decision, replay_record_decision, should_capture_first_downgrade,
};
pub use remote_metadata::remote_metadata_partition;
pub use remote_read::remote_read_relative_offset;
pub use remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision};
pub use restore::{restore_batch_step, restore_record_coordinates};
pub use retention::{
    RetentionPrefix, local_retention_prefix, retention_delete_target, retention_prefix,
};
pub use schema::{SchemaFailureDecision, SchemaFailureKind, schema_failure_decision};
pub use storage::future_log_swap_admission;
pub use stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
pub use transaction::{
    IdleTransactionState, NO_TRANSACTION_TIMEOUT_MS, TransactionCompletionDecision,
    TransactionIdentity, TransactionSnapshot, resolve_transaction_timeout,
    should_abort_idle_transaction, transaction_completion_decision,
};
pub use vote::{
    VoteAdmissionDecision, VoteEncodeDecision, VoteWireDecision, vote_admission_decision,
    vote_encode_decision, vote_wire_decision,
};
pub use wal::{
    WalFetchAdmission, exact_wal_batch_range, select_wal_voter_index, wal_fetch_admission,
};
