//! The `ConsensusModel` itself and the four bounded configurations the checker
//! runs. The bounds are the whole reason a config is a named constructor rather
//! than a literal, so they are gathered in one file away from the transition
//! logic they constrain.

use krabka_raft::kraft::types::{Epoch, NodeId};
use krabka_units::prelude::{Time, TimeExt as _};

pub struct ConsensusModel {
    pub voter_ids: Vec<NodeId>,
    /// Maximum client appends issued across a path. `0` disables the
    /// client-append and linearizability machinery entirely, which leaves the
    /// focus on election and log matching.
    pub max_appends: u32,
    /// Cap on in-flight messages. This bounds the state space.
    pub max_inflight: usize,
    /// Cap on the leader epoch. This bounds the state space.
    pub max_epoch: Epoch,
    /// Enables the message-loss and message-duplication faults.
    pub enable_loss_dup: bool,
    /// Maximum number of nodes crashed at the same time. `0` means no
    /// crashes.
    pub max_crashes: usize,
    /// Offer appends through every live node, not just direct leader appends.
    pub enable_append_via: bool,
}

impl ConsensusModel {
    /// Election and log-matching focus. There are NO client appends, so the
    /// space stays the small, fast one from the scaffolding task. This exercises
    /// leader election and log replication safety across N voters.
    pub fn elections(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends: 0,
            max_inflight: 3,
            max_epoch: 2,
            enable_loss_dup: false,
            max_crashes: 0,
            enable_append_via: false,
        }
    }

    /// Linearizability focus. Client appends are ENABLED, but the bounds are
    /// tight, because the linearizability tester keeps the history in the
    /// fingerprinted state and so makes the space far larger. The bounds stay
    /// small enough to exhaust the space exactly.
    pub fn linearizable(voter_ids: &[NodeId], max_appends: u32) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends,
            max_inflight: 3,
            max_epoch: 2,
            enable_loss_dup: false,
            max_crashes: 0,
            enable_append_via: false,
        }
    }

    /// Fault-injection focus: message loss, message duplication, and a single
    /// crash and recover, over very tight bounds, because faults multiply the
    /// action space. There are no client appends. This exercises election and
    /// log-matching safety under an adversarial network, which is where the
    /// bounded space stays exhaustible.
    pub fn faults(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends: 0,
            max_inflight: 2,
            max_epoch: 2,
            enable_loss_dup: true,
            max_crashes: 1,
            enable_append_via: false,
        }
    }

    /// Diskless append focus. Client appends are offered through every live
    /// node, which models stateless appenders, but the ordered controller log
    /// stays the linearization point.
    pub fn append_via(voter_ids: &[NodeId], max_appends: u32) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends,
            max_inflight: 3,
            max_epoch: 2,
            enable_loss_dup: false,
            max_crashes: 0,
            enable_append_via: true,
        }
    }

    /// The base election timeout configured for node `id`. It is staggered by
    /// id, so timer ties break deterministically. The model's clock stays a
    /// constant [`SimInstant`], and this is the extent the core is constructed
    /// with.
    pub(super) fn election_timeout_of(id: NodeId) -> Time {
        Time::from_millis(i64::try_from(1000 + id.0 * 50).unwrap_or(i64::MAX))
    }

    pub(super) fn voter_set(&self) -> krabka_metadata::voters::VoterSet {
        krabka_metadata::voters::VoterSet::from_voters(self.voter_ids.iter().map(|&id| {
            krabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: Vec::new(),
                kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }
}
