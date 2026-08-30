//! Formally verified pure kernels shared by Krabka crates.
//!
//! Every function here is a total, synchronous, allocation-light kernel, and
//! Creusot proves its functional contract. See `docs/verification.md`. Host
//! crates call through to these functions, and there are no duplicate bodies
//! anywhere.
#![doc(html_root_url = "https://docs.rs/krabka-verified/0.5.0")]

pub mod broker;
pub mod compaction;
pub mod consensus;
pub mod leader_epoch;
pub mod log_index;
pub mod offset_allocator;
pub mod producer;
pub mod stretch;
pub mod throttle;

pub use broker::{
    FetchVisibility, FetchWatermarks, delete_records_offset_out_of_range, delete_records_target,
    effective_share_backlog, fetch_visibility,
};
pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{
    election_jitter_ms, handoff_high_watermark, log_is_up_to_date, recompute_high_watermark,
};
pub use leader_epoch::{EpochEntry, epoch_and_offset_for_entries};
pub use log_index::{offset_index_lookup, offset_index_position_at_or_after, time_index_lookup};
pub use offset_allocator::reserve_offsets;
pub use producer::{
    ProducerBatch, ProducerDecision, decrement_sequence, increment_sequence, producer_decision,
};
pub use stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
