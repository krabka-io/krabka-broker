//! The `KRaft` quorum state machine: `on_event(event, log, now) -> Vec<Action>`.
//!
//! This module root holds the [`QuorumStateMachine`] type, its constructor
//! and accessors, and the `on_event` dispatch. One submodule per event
//! family holds the handlers: `membership` applies voter records,
//! `vote_request` answers an inbound vote, `election` runs the pre-vote and
//! vote rounds, `leadership` reacts to a leader announcing its epoch, and
//! `replication` serves and answers Fetch.

use krabka_units::prelude::{Time, TimeExt as _};
use krabka_voters::VoterSet;

use crate::{
    action::{Action, TimerKind},
    event::{Event, LogEnd},
    role::Role,
    types::{Epoch, LogView, NodeId, QuorumState, SimInstant},
};

mod election;
mod leadership;
mod membership;
mod replication;
mod vote_request;

#[cfg(test)]
mod test_support;

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`.
///
/// This is Raft's randomized backoff, made reproducible for the deterministic
/// sims. Different nodes get different spreads, and so does the same node
/// across re-election epochs. Closely-synchronized voters therefore do not arm
/// their election timers in lockstep and split the vote indefinitely.
///
/// Both the pure core and the async engine's initial timer arm call this
/// function, so production self-staggers without per-node config.
#[must_use]
pub fn election_jitter_ms(me: NodeId, epoch: Epoch, base_ms: u64) -> u64 {
    krabka_verified::election_jitter_ms(me.0, epoch, base_ms)
}

/// The hand-rolled KIP-595 + KIP-996 quorum state machine.
///
/// The state machine is pure and deterministic. It consumes [`Event`]s, reads
/// the log through [`LogView`], takes the current time as an injected
/// [`SimInstant`], and produces a list of [`Action`]s for the caller to
/// execute. It never touches the clock, the wire, or the log bytes directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QuorumStateMachine {
    me: NodeId,
    state: QuorumState,
    /// The other side of the one in-flight KIP-853 transition. Requests from
    /// this explicitly adjacent set remain valid until the voter record commits.
    adjacent_voters: Option<VoterSet>,
    role: Role,
    /// Base election timeout, in whole milliseconds.
    ///
    /// This field holds a raw value and not a [`Time`]. Quantities store `f64`,
    /// so a [`Time`] field would cost this struct its `Eq` and `Hash` derives.
    /// The `stateright` model checker needs those derives on every state it
    /// explores. Whole milliseconds is also the domain of the verified jitter
    /// kernel. [`QuorumStateMachine::new`] converts at that boundary.
    election_timeout_ms: u64,
}

#[derive(Clone, Copy)]
struct VoteRequest {
    from: NodeId,
    cluster_id: Option<uuid::Uuid>,
    voter_id: NodeId,
    voter_directory_id: uuid::Uuid,
    candidate_epoch: Epoch,
    candidate: NodeId,
    candidate_directory_id: uuid::Uuid,
    candidate_log_end: LogEnd,
    pre_vote: bool,
}

impl QuorumStateMachine {
    /// `election_timeout` is the base extent of an election timer, before
    /// jitter; callers vary it per node for liveness.
    ///
    /// This constructor rounds the value to whole milliseconds. That is the
    /// domain of both the verified jitter kernel and the [`SimInstant`] clock.
    #[must_use]
    pub fn new(me: NodeId, state: QuorumState, election_timeout: Time) -> Self {
        let observer = !state.voters.contains(me);
        let role = if observer {
            Role::Observer {
                leader_id: None,
                fetch_deadline: SimInstant(0),
            }
        } else {
            Role::default()
        };
        Self {
            me,
            state,
            adjacent_voters: None,
            role,
            election_timeout_ms: u64::try_from(election_timeout.millis_i64()).unwrap_or(0),
        }
    }

    #[must_use]
    pub fn quorum_state(&self) -> &QuorumState {
        &self.state
    }
    /// This replica's own node id.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }
    #[must_use]
    pub fn role(&self) -> &Role {
        &self.role
    }
    #[must_use]
    pub fn is_voter(&self) -> bool {
        self.state.voters.contains(self.me)
    }

    #[cfg(test)]
    pub(crate) fn force_epoch(&mut self, e: Epoch) {
        self.state.leader_epoch = e;
    }

    /// The deadline for an election timer armed at `now`.
    ///
    /// This method adds deterministic per-`(node, epoch)` jitter, the standard
    /// Raft randomized backoff made deterministic for the sims. Competing
    /// voters then do not arm their election timers in lockstep. Without the
    /// jitter, a bare majority of in-process or closely-synchronized voters,
    /// for example exactly 2 of a 3-voter set, splits the vote every round.
    /// Both become candidates, both self-vote, and neither reaches a majority.
    /// Elections then livelock until natural skew breaks the tie.
    ///
    /// The whole sum stays in integer milliseconds. The jitter is a verified
    /// integer kernel, and [`SimInstant`] is a coordinate on a millisecond
    /// timeline and not an extent.
    fn election_deadline(&self, now: SimInstant) -> SimInstant {
        now.saturating_add_ms(
            self.election_timeout_ms
                + election_jitter_ms(self.me, self.state.leader_epoch, self.election_timeout_ms),
        )
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, role = self.role.name())
    )]
    pub fn on_event(&mut self, event: Event, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        match event {
            Event::ReceiveVoteRequest {
                from,
                cluster_id,
                voter_id,
                voter_directory_id,
                candidate_epoch,
                candidate,
                candidate_directory_id,
                candidate_log_end,
                pre_vote,
            } => self.handle_vote_request(
                VoteRequest {
                    from,
                    cluster_id,
                    voter_id,
                    voter_directory_id,
                    candidate_epoch,
                    candidate,
                    candidate_directory_id,
                    candidate_log_end,
                    pre_vote,
                },
                log,
                now,
            ),
            Event::ElectionTimeout => self.handle_election_timeout(log, now),
            Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted,
            } => self.handle_vote_response(log, from, epoch, vote_granted, now),
            Event::ReceiveBeginQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_begin_quorum_epoch(leader_id, leader_epoch, now),
            Event::ReceiveEndQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_end_quorum_epoch(log, leader_id, leader_epoch, now),
            Event::ReceiveFetch {
                from,
                fetch_epoch,
                fetch_offset,
            } => self.handle_fetch(log, from, fetch_epoch, fetch_offset),
            Event::ReceiveFetchResponse {
                leader_id,
                leader_epoch,
                diverging,
            } => self.handle_fetch_response(leader_id, leader_epoch, diverging, now),
            Event::FetchTimeout => self.handle_fetch_timeout(log, now),
        }
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, from_epoch = self.state.leader_epoch, to_epoch = epoch)
    )]
    fn transition_to_unattached(
        &mut self,
        epoch: Epoch,
        deadline: SimInstant,
        actions: &mut Vec<Action>,
    ) {
        self.state.leader_epoch = epoch;
        self.state.leader_id = None;
        self.state.voted_key = None;
        self.role = Role::Unattached {
            election_deadline: deadline,
        };
        actions.push(Action::PersistQuorumState);
        actions.push(Action::TransitionedTo("Unattached"));
        // Arm the election timer so a fenced/stepped-down node will eventually
        // re-elect if no leader emerges (without this it would deadlock).
        actions.push(Action::ResetTimer {
            kind: TimerKind::Election,
            deadline,
        });
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{millis, secs};

    use super::*;
    use crate::core::test_support::{FakeLog, voters};

    #[test]
    fn election_deadline_is_the_configured_extent_plus_integer_jitter() {
        // The constructor takes an extent, but the armed deadline must land on
        // exactly `now + base_ms + jitter_ms` — the same integer timeline the
        // verified jitter kernel and `SimInstant` are defined over. A rounding
        // shift here would change which node wins a deterministic election.
        for (name, timeout, base_ms) in [
            ("whole seconds", secs(1), 1000u64),
            ("sub-second extent", millis(250), 250),
        ] {
            let mut m = QuorumStateMachine::new(
                NodeId(1),
                QuorumState::bootstrap(
                    uuid::Uuid::nil(),
                    voters(&[NodeId(1), NodeId(2), NodeId(3)]),
                ),
                timeout,
            );
            let log = FakeLog {
                end: 5,
                last_epoch: 1,
            };
            let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
            let armed = actions.iter().find_map(|a| match a {
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline,
                } => Some(*deadline),
                _ => None,
            });
            let expected = SimInstant(2000 + base_ms + election_jitter_ms(NodeId(1), 0, base_ms));
            check!(armed == Some(expected), "case {name}");
        }
    }

    #[test]
    fn election_jitter_is_deterministic_hash_in_range() {
        // Pin the exact deterministic jitter so a constant-return regression
        // (no jitter at all → split-vote livelock) is caught. The values are the
        // integer hash of (node, epoch) mod base_ms; they are non-zero and
        // node-dependent, so both "always 0" and "always 1" are distinguished.
        for (name, node, epoch, base_ms, expected) in [
            ("first node", NodeId(1), 0, 1000, 485),
            ("different node", NodeId(2), 0, 1000, 354),
            ("next epoch", NodeId(1), 1, 1000, 446),
            ("zero base", NodeId(1), 0, 0, 0),
        ] {
            check!(
                election_jitter_ms(node, epoch, base_ms) == expected,
                "case {name}"
            );
        }
    }
}
