//! Translation of the actions the consensus core emits into bus traffic, timer
//! updates, and log bookkeeping, plus the replication the harness performs on a
//! follower's behalf. This is where the core's outputs become the peers' inputs.

use krabka_raft::kraft::{
    action::{Action, TimerKind},
    event::Event,
    types::{Epoch, NodeId},
};

use super::{cluster::Sim, node::Message, node_log::SimNodeLog};

impl<L: SimNodeLog> Sim<L> {
    /// Broadcasts a vote or pre-vote request from `id` to every other voter.
    fn broadcast_vote_request(&mut self, id: NodeId, epoch: Epoch, pre_vote: bool) {
        let cand_log = self.nodes[&id].log.log_end();
        for peer in self.voter_ids.clone() {
            if peer != id {
                self.send(
                    id,
                    peer,
                    Event::ReceiveVoteRequest {
                        from: id,
                        cluster_id: None,
                        voter_id: peer,
                        voter_directory_id: uuid::Uuid::nil(),
                        candidate_epoch: epoch,
                        candidate: id,
                        candidate_directory_id: uuid::Uuid::nil(),
                        candidate_log_end: cand_log,
                        pre_vote,
                    },
                );
            }
        }
    }

    /// Translates a single emitted `Action` from node `id` into bus messages,
    /// timer updates, and log and HWM bookkeeping.
    pub(super) fn apply_action(&mut self, id: NodeId, action: Action) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => {
                self.broadcast_vote_request(id, epoch, pre_vote);
            }
            Action::ReplyVote { to, epoch, granted } => {
                self.send(
                    id,
                    to,
                    Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                );
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                for peer in self.all_node_ids() {
                    if peer != id {
                        self.send(
                            id,
                            peer,
                            Event::ReceiveBeginQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        );
                    }
                }
            }
            Action::SendEndQuorumEpoch { epoch } => {
                for peer in self.all_node_ids() {
                    if peer != id {
                        self.send(
                            id,
                            peer,
                            Event::ReceiveEndQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        );
                    }
                }
            }
            Action::SendFetch { leader_id } => {
                // The follower fetches from the leader. Model replication first:
                // copy any leader log entries this follower is missing, then send
                // the fetch carrying the follower's (now-advanced) tip so the
                // leader can advance its HWM.
                self.replicate_from_leader(id, leader_id);
                let (fetch_epoch, fetch_offset) = {
                    let log = &self.nodes[&id].log;
                    (log.last_epoch(), log.end_offset())
                };
                self.send(
                    id,
                    leader_id,
                    Event::ReceiveFetch {
                        from: id,
                        fetch_epoch,
                        fetch_offset,
                    },
                );
            }
            Action::AppendLeaderChange { epoch } => {
                // The new leader appends one control record in its current epoch.
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.high_watermark = hwm;
                node.log.advance_hwm(hwm);
                // In KRaft the new high watermark rides along on the leader's
                // next fetch response, so every follower eventually learns it —
                // including a caught-up follower that is long-polling and would
                // otherwise never re-fetch. Model that by pushing the committed
                // boundary to every peer's log now (each `advance_hwm` is
                // monotonic and clamped to that peer's own replicated log end, so
                // a lagging follower only commits what it actually holds).
                for peer in self.all_node_ids() {
                    if peer != id && !self.partitioned.contains(&peer) {
                        let p = self.nodes.get_mut(&peer).unwrap();
                        p.log.advance_hwm(hwm);
                        p.high_watermark = hwm.min(p.log.end_offset());
                    }
                }
            }
            Action::TruncateTo(point) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.truncate_to(point.offset);
            }
            Action::ResetTimer { kind, deadline } => {
                let node = self.nodes.get_mut(&id).unwrap();
                match kind {
                    TimerKind::Election => node.election_deadline = Some(deadline),
                    TimerKind::Fetch => node.fetch_deadline = Some(deadline),
                    TimerKind::CheckQuorum => node.check_quorum_deadline = Some(deadline),
                }
            }
            // Pure bookkeeping signals with no cross-node effect in the sim.
            Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

    /// Copies the log entries from `leader` that `follower` is missing, so the
    /// follower logs converge and the follower's fetch offset advances toward
    /// the leader's end.
    ///
    /// The method respects the epochs, because it delegates the byte-faithful
    /// copy and the divergence truncation to the log impl. It runs only when
    /// `leader` actually believes it is the leader and neither endpoint is
    /// partitioned.
    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if follower == leader {
            return;
        }
        if self.partitioned.contains(&follower) || self.partitioned.contains(&leader) {
            return;
        }
        if !self.nodes[&leader].machine.role().is_leader() {
            return;
        }
        // Two distinct nodes need simultaneous access (follower mut, leader ref).
        // `BTreeMap` has no stable disjoint-borrow API, so lift the follower out,
        // replicate against the still-resident leader, then put it back.
        let leader_hwm = self.leader_high_watermark(leader);
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        // The follower learns the leader's committed offset on each fetch (the
        // fetch response carries the leader's high watermark in real KRaft), so
        // its own committed-read boundary tracks the consensus HWM, bounded by
        // what it has actually replicated.
        follower_node.log.advance_hwm(leader_hwm);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    /// Enqueues an event for delivery to `dst`. If either endpoint is currently
    /// partitioned, the harness silently drops the message, the same way a real
    /// network partition does.
    pub(super) fn send(&mut self, src: NodeId, dst: NodeId, event: Event) {
        if self.partitioned.contains(&src) || self.partitioned.contains(&dst) {
            return;
        }
        self.queue.push_back(Message { src, dst, event });
    }

    fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }
}
