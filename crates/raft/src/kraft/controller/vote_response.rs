//! The candidate side of a Vote: the answer a voter sent back, read as
//! Kafka's `KafkaRaftClient.handleVoteResponse` reads it.
//!
//! The answer carries the voter's epoch and the leader it knows as well as the
//! grant. Kafka's `maybeHandleCommonResponse` acts on those before it counts
//! the grant: a nominee that a voter names a leader to follows that leader
//! instead of waiting for its own election timeout to run out.

use {
    super::{Engine, quorum_requests::UNKNOWN_LEADER_EPOCH},
    crate::kraft::{
        event::Event,
        transport::wire,
        types::{Epoch, NodeId},
    },
};

#[cfg(test)]
mod tests;

/// What a Vote answer says, as far as the step it causes depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteAnswer {
    /// The partition error code.
    pub error_code: i16,
    /// Whether the voter granted the vote.
    pub granted: bool,
    /// The leader the voter knows, if any.
    pub leader_id: Option<NodeId>,
    /// The voter's epoch.
    pub epoch: Epoch,
}

/// Where this replica stands when the answer arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteAsker {
    /// This replica.
    pub me: NodeId,
    /// This replica's epoch.
    pub epoch: Epoch,
    /// The leader this replica knows for its epoch, Kafka's
    /// `QuorumState.hasLeader`.
    pub known_leader: Option<NodeId>,
}

/// What a Vote answer does to the replica that asked for the vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResponseStep {
    /// The answer is no longer relevant: it comes from an older epoch, or it
    /// says that the voter does not know the epoch yet.
    Ignore,
    /// Follow the leader the answer names.
    Follow { leader_id: NodeId, epoch: Epoch },
    /// Move to the newer epoch the answer names, which has no leader yet.
    Unattached { epoch: Epoch },
    /// A successful answer in this replica's epoch: count the vote.
    Count { epoch: Epoch, granted: bool },
}

/// Kafka's `maybeHandleCommonResponse` and then `handleVoteResponse`.
///
/// An older epoch or `UNKNOWN_LEADER_EPOCH` is ignored. A newer epoch goes
/// through `maybeTransition`: the replica follows the leader it names, or
/// becomes Unattached in it when it names none. In the replica's own epoch,
/// an answer that names a leader while the replica knows none moves it to
/// that leader, whatever its error code: Kafka takes that step through
/// `maybeTransition` for `FENCED_LEADER_EPOCH` and `NOT_LEADER_OR_FOLLOWER`
/// and through its own branch for any other code, and the grant then no
/// longer counts, since the replica is no longer a nominee. Any other
/// successful answer is counted, and any other error is dropped.
///
/// A replica never follows itself: an answer that names this replica as the
/// leader of an epoch it does not lead is ignored.
#[must_use]
pub fn vote_response_step(asker: VoteAsker, answer: VoteAnswer) -> VoteResponseStep {
    let VoteAnswer {
        error_code,
        granted,
        leader_id,
        epoch,
    } = answer;
    let follow = |leader_id: Option<NodeId>| match leader_id {
        Some(leader_id) if leader_id != asker.me => VoteResponseStep::Follow { leader_id, epoch },
        _ => VoteResponseStep::Ignore,
    };
    if epoch < asker.epoch || error_code == UNKNOWN_LEADER_EPOCH {
        VoteResponseStep::Ignore
    } else if epoch > asker.epoch {
        if leader_id.is_some() {
            follow(leader_id)
        } else {
            VoteResponseStep::Unattached { epoch }
        }
    } else if leader_id.is_some() && asker.known_leader.is_none() {
        follow(leader_id)
    } else if error_code == 0 {
        VoteResponseStep::Count { epoch, granted }
    } else {
        VoteResponseStep::Ignore
    }
}

impl Engine {
    /// (Nominee side) apply a voter's answer to our Vote request.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, from = from.0))]
    pub fn on_vote_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::Vote {
            error_code,
            granted,
            leader,
        }) = wire::PeerResponse::decode_vote(body)
        else {
            return;
        };
        let asker = VoteAsker {
            me: self.me,
            epoch: self.core.quorum_state().leader_epoch,
            known_leader: self.core.known_leader(),
        };
        let answer = VoteAnswer {
            error_code,
            granted,
            leader_id: leader.leader_id,
            epoch: leader.epoch,
        };
        match vote_response_step(asker, answer) {
            VoteResponseStep::Ignore => {}
            VoteResponseStep::Follow { leader_id, epoch } => {
                self.remember_leader_endpoint(&leader);
                self.on_event(Event::ReceiveBeginQuorumEpoch {
                    leader_id,
                    leader_epoch: epoch,
                });
            }
            VoteResponseStep::Unattached { epoch } => {
                self.on_event(Event::ReceiveLeaderlessEpoch { epoch });
            }
            VoteResponseStep::Count { epoch, granted } => {
                self.on_event(Event::ReceiveVoteResponse {
                    from,
                    epoch,
                    vote_granted: granted,
                });
            }
        }
    }
}
