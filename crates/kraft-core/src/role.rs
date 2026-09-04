//! The replica's volatile role within the current epoch and its per-role state.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{NodeId, SimInstant};

/// Per-follower replication progress that a leader tracks for the HWM.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplicaProgress {
    /// Highest offset the follower has acknowledged, that is, its fetch offset.
    pub fetch_offset: i64,
    /// Logical instant the leader received the most recent Fetch from this follower.
    pub last_fetch: SimInstant,
    /// Logical instant this follower was known to be caught up with the leader's log end.
    pub last_caught_up: SimInstant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Role {
    /// Knows the epoch, no leader yet. May hold a non-binding pre-vote grant.
    Unattached { election_deadline: SimInstant },
    /// Cast a binding vote this epoch; now waits for a leader.
    Voted { election_deadline: SimInstant },
    /// Has a leader for the epoch and fetches from it.
    Follower {
        leader_id: NodeId,
        fetch_deadline: SimInstant,
    },
    /// KIP-996 pre-vote candidate that collects non-binding grants.
    Prospective {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Real candidacy: the epoch is bumped and the replica voted for itself.
    Candidate {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Won the election; tracks follower progress for HWM.
    Leader {
        replicas: BTreeMap<NodeId, ReplicaProgress>,
        /// Voters that have fetched inside the current check-quorum window.
        ///
        /// This is Kafka's `LeaderState.fetchedVoters`: it never contains the
        /// leader itself, and it is emptied — and the check-quorum timer
        /// re-armed — the moment it reaches the majority the leader needs
        /// besides its own vote. A leader that reaches the deadline with the
        /// set still short of that count has lost the quorum and resigns.
        fetched_voters: BTreeSet<NodeId>,
        high_watermark: i64,
        /// Log end offset at the moment of promotion, that is, where this
        /// leader's `LeaderChange` record sits. That record is the first
        /// current-epoch record.
        ///
        /// The HWM may only advance past this offset. This enforces Raft Fig.8
        /// leader completeness: a current-epoch entry must be
        /// majority-replicated before commit.
        epoch_start_offset: i64,
    },
    /// Stepped down: told the voters to elect, and waits for its own election
    /// timer to start a pre-vote round.
    ///
    /// A leader reaches this variant when its check-quorum window expires. It
    /// no longer serves Fetch, no longer advances the high watermark, and no
    /// longer claims the leadership in `QuorumState`, so an isolated old leader
    /// stops answering as leader for its epoch.
    Resigned,
    /// Not in the voter set; only ever fetches.
    Observer {
        leader_id: Option<NodeId>,
        fetch_deadline: SimInstant,
    },
}

impl Default for Role {
    fn default() -> Self {
        Role::Unattached {
            election_deadline: SimInstant(0),
        }
    }
}

impl Role {
    #[must_use]
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Role::Unattached { .. } => "Unattached",
            Role::Voted { .. } => "Voted",
            Role::Follower { .. } => "Follower",
            Role::Prospective { .. } => "Prospective",
            Role::Candidate { .. } => "Candidate",
            Role::Leader { .. } => "Leader",
            Role::Resigned => "Resigned",
            Role::Observer { .. } => "Observer",
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    /// Every role reports its own name. These strings reach operators through
    /// metrics and logs, so one standing in for another is a real confusion.
    #[test]
    fn each_role_reports_its_own_name() {
        let names = [Role::default().name(), Role::Resigned.name()];
        assert2::assert!(names == ["Unattached", "Resigned"]);
        // Distinct, not merely non-empty: a single constant would satisfy the
        // latter for every variant.
        assert2::assert!(names[0] != names[1]);
    }

    #[test]
    fn role_defaults_to_unattached() {
        let r = Role::default();
        assert2::assert!((matches!(r, Role::Unattached { .. }), r.is_leader()) == (true, false));
    }
}
