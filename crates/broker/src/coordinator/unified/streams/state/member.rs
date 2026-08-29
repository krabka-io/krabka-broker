//! One streams-group member: its identity, its epochs, its three per-role task
//! sets, and the reconciliation state of its active tasks.
//!
//! The member is the unit the KIP-848-style epoch exchange acts on, so the
//! [`StreamsMemberAssignmentState`] enum and the `i8` conversions that
//! persistence needs live beside it rather than in the group state machine.

use std::{collections::BTreeMap, time::Instant};

use krabka_log::Offset;

/// The reconciliation state of one streams-group member's **active** task set.
///
/// It mirrors KIP-848's `MemberAssignmentState`. Standby and warmup tasks take
/// no part in it.
///
/// Persistence stores this as a raw `i8`. [`as_i8`](Self::as_i8) and
/// [`from_i8`](Self::from_i8) convert it without coupling this module to the
/// persistence layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsMemberAssignmentState {
    /// The member's active tasks match its target, and nothing is pending.
    #[default]
    Stable = 0,
    /// The member still owns active tasks the new target took away from it.
    /// It must revoke them and acknowledge that in a heartbeat before it
    /// advances.
    UnrevokedActiveTasks = 1,
    /// The member's target includes active tasks that another member still
    /// owns and has not revoked. The member must wait for their release.
    UnreleasedActiveTasks = 2,
}

impl StreamsMemberAssignmentState {
    /// Raw `i8` discriminant for the persistence layer.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        self as i8
    }

    /// Inverse of [`as_i8`](Self::as_i8). It returns `None` for an unknown
    /// discriminant instead of a panic, so that the caller, the actor, decides
    /// how to report a corrupt persisted record.
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Stable),
            1 => Some(Self::UnrevokedActiveTasks),
            2 => Some(Self::UnreleasedActiveTasks),
            _ => None,
        }
    }
}

/// One member of a streams group.
///
/// A member holds three disjoint sets of tasks by role: `active`, `standby`,
/// and `warmup`. Each set keys by subtopology id and holds a sorted, deduped
/// partition list. The `active_pending_revocation` map holds the active tasks
/// the member must give up before it can advance its epoch.
#[derive(Debug, Clone)]
pub struct StreamsMemberState {
    // --- identity ---
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    /// The Streams `process.id`, a stable per-process UUID string. The
    /// assignor reads it to co-locate the tasks of one process.
    pub process_id: String,
    /// Optional `(host, port)` the member advertises for interactive-query
    /// routing.
    pub user_endpoint: Option<(String, u32)>,
    /// Arbitrary `(key, value)` client tags for rack-aware and custom
    /// assignment.
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,

    // --- epochs ---
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    /// The topology epoch this member last acknowledged.
    pub topology_epoch: i32,

    // --- assignment ---
    pub assignment_state: StreamsMemberAssignmentState,
    /// Assigned active tasks: `subtopology_id` -> sorted, deduped partitions.
    pub active: BTreeMap<String, Vec<i32>>,
    /// Assigned standby tasks.
    pub standby: BTreeMap<String, Vec<i32>>,
    /// Assigned warmup tasks.
    pub warmup: BTreeMap<String, Vec<i32>>,
    /// Active tasks the member must revoke before it advances (KIP-848).
    pub active_pending_revocation: BTreeMap<String, Vec<i32>>,

    // --- reported catch-up progress (for warmup -> active promotion) ---
    /// `(subtopology, partition)` -> the changelog position the member last
    /// reported for that task.
    pub task_offsets: BTreeMap<(String, i32), Offset>,
    /// `(subtopology, partition)` -> the changelog end offset the member last
    /// reported for that task.
    pub task_end_offsets: BTreeMap<(String, i32), Offset>,

    pub last_seen: Instant,
}

impl StreamsMemberState {
    /// Constructs a newly joining member at epoch 0 with no assignment.
    ///
    /// When the client supplies no `process_id`, this method synthesizes a
    /// random UUID. A caller that already knows the process id should set the
    /// field afterwards.
    pub fn joining(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            process_id: uuid::Uuid::new_v4().to_string(),
            user_endpoint: None,
            client_tags: Vec::new(),
            rebalance_timeout_ms: 0,
            member_epoch: 0,
            previous_member_epoch: 0,
            topology_epoch: 0,
            assignment_state: StreamsMemberAssignmentState::Stable,
            active: BTreeMap::new(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: BTreeMap::new(),
            task_offsets: BTreeMap::new(),
            task_end_offsets: BTreeMap::new(),
            last_seen: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn assignment_state_i8_roundtrips() {
        for s in [
            StreamsMemberAssignmentState::Stable,
            StreamsMemberAssignmentState::UnrevokedActiveTasks,
            StreamsMemberAssignmentState::UnreleasedActiveTasks,
        ] {
            assert!(StreamsMemberAssignmentState::from_i8(s.as_i8()) == Some(s));
        }
        assert!(StreamsMemberAssignmentState::from_i8(99).is_none());
        assert!(StreamsMemberAssignmentState::default() == StreamsMemberAssignmentState::Stable);
    }
}
