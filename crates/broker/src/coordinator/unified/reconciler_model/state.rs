//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member or one advertisement out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use crate::coordinator::unified::actor::reconciliation_model_support::{
    group_projection, member_projection, model_actions,
};

member_projection! {
    /// Per-member coordinator-side projection, which maps a single topic to a
    /// `Vec<i32>`.
    pub(super) struct MemberProj {
    }
}

group_projection! { pub(super) struct ReconState {
    members: Vec<MemberProj>,
} }

model_actions! { pub(super) enum ReconAction {
} }
