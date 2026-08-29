//! Per-group state for KIP-848 next-gen consumer groups. Exactly one
//! `actor::GroupActor` task owns this state. It is never shared.
//!
//! This file is the module root. The [`MemberState`] record and its cached
//! subscription regex live in `member`, the [`GroupState`] container and its
//! membership transitions in `group`, and the target-assignment
//! reconciliation in `reconcile`. [`TargetAssignment`] stays here: it is a
//! leaf record with no behaviour of its own.

use std::collections::HashMap;

use krabka_protocol::primitives::uuid::Uuid;

mod group;
mod member;
mod reconcile;

#[cfg(test)]
mod test_support;

pub use self::{
    group::GroupState,
    member::{ClassicMemberFacade, CompiledRegex, MemberState},
};

#[derive(Debug, Clone, Default)]
pub struct TargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}
