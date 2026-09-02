//! Classic-protocol Join, Sync, Heartbeat, Leave, and offset-validate logic
//! against [`ClassicState`]. These are pure transitions that return a
//! *disposition*, which the actor turns into either an immediate reply or a
//! parked `oneshot`. They come verbatim from the old
//! `handlers/{join_group,sync_group,heartbeat,leave_group}.rs`
//! and `offset_commit::validate`. The actor (`super::actor`) owns the
//! park/wake plumbing and the rebalance-deadline timer that drives completion.
//!
//! This file is the module root. Each classic operation lives in its own
//! submodule, and the root re-exports the transitions the actor calls.
//!
//! [`ClassicState`]: super::classic_state::ClassicGroup

mod heartbeat;
mod join;
mod leave;
mod offset_validation;
mod sync;

#[cfg(test)]
mod test_support;

pub(super) use self::{
    heartbeat::handle_heartbeat,
    join::{CompleteError, JoinAction, build_join_result, handle_join, try_complete},
    leave::handle_leave,
    offset_validation::validate_commit,
    sync::{SyncAction, handle_sync, read_sync_result},
};
