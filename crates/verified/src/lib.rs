//! Formally verified pure kernels shared by Krabka crates.
//!
//! Every function here is a total, synchronous, allocation-light kernel, and
//! Creusot proves its functional contract. See `docs/verification.md`. Host
//! crates call through to these functions, and there are no duplicate bodies
//! anywhere.
#![doc(html_root_url = "https://docs.rs/krabka-verified/0.5.2")]

pub mod audit;
pub mod authz;
pub mod broker;
pub mod chain;
pub mod compaction;
pub mod consensus;
pub mod diskless;
pub mod isr;
pub mod leader_epoch;
pub mod log_index;
pub mod offset_allocator;
pub mod producer;
pub mod raft;
pub mod stretch;
pub mod throttle;
pub mod transaction;
pub mod wal;

pub use audit::{SpoolAppendDecision, spool_append_decision};
pub use authz::{AclDecision, acl_decision};
pub use broker::{
    FetchVisibility, FetchWatermarks, delete_records_offset_out_of_range, delete_records_target,
    effective_share_backlog, fetch_visibility,
};
pub use chain::{ChainStep, chain_step, select_chain_tip};
pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{
    election_has_quorum, election_jitter_ms, handoff_high_watermark, log_is_up_to_date,
    majority_size, recompute_high_watermark,
};
pub use diskless::{DisklessTrimDecision, diskless_trim_decision};
pub use leader_epoch::{EpochEntry, epoch_and_offset_for_entries};
pub use log_index::{offset_index_lookup, offset_index_position_at_or_after, time_index_lookup};
pub use offset_allocator::reserve_offsets;
pub use producer::{
    ProducerBatch, ProducerDecision, decrement_sequence, increment_sequence, producer_decision,
};
pub use stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
pub use transaction::{
    TransactionCompletionDecision, TransactionIdentity, TransactionSnapshot,
    transaction_completion_decision,
};
pub use wal::{WalFetchAdmission, wal_fetch_admission};
