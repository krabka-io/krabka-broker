//! The in-memory message bus: delivery, and the application of every [`Action`]
//! the state machine returns.
//!
//! This module holds the half of the harness that moves data. It hands one
//! [`Event`] to a node, turns each returned action into queued messages, log
//! appends, truncations, and timer arms, and it holds the two replay faults
//! that deliver the queue back-to-front or deliver one message twice.

use super::{
    Sim,
    node::Message,
    trace::{TraceAction, event_label},
};
use crate::{
    action::{Action, TimerKind},
    event::Event,
    role::Role,
    types::{Epoch, LogView, NodeId},
};

impl Sim {
    pub(super) fn deliver(&mut self, msg: &Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        let label = event_label(&msg.event);
        let (src, dst) = (msg.src, msg.dst);
        self.step(dst, msg.event);
        self.record(
            TraceAction::Deliver {
                src: src.0,
                dst: dst.0,
                event: label.clone(),
            },
            format!("N{src} → N{dst}: {label}"),
        );
        self.record_new_leaders();
    }

    pub(super) fn step(&mut self, id: NodeId, event: Event) {
        let now = self.now;
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        let actions = {
            let node = self.nodes.get_mut(&id).unwrap();
            node.machine.on_event(event, &node.log, now)
        };
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::TruncateTo(point) => Some(*point),
                _ => None,
            });
            let leader_epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
            if self.nodes[&id].machine.role().is_leader() {
                let leader_end = self.nodes[&id].log.end_offset();
                let follower_end = self.nodes[&follower].log.end_offset();
                let has_new_data = follower_end < leader_end;
                if diverging.is_some() || has_new_data {
                    self.send(
                        id,
                        follower,
                        Event::ReceiveFetchResponse {
                            leader_id: id,
                            leader_epoch,
                            diverging,
                        },
                    );
                }
            }
        }
        for action in actions {
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(id, action);
        }
        self.reconcile_timers_for_role(id);
    }

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
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.high_watermark = hwm;
                for peer in self.all_node_ids() {
                    if peer != id && !self.partitioned.contains(&peer) {
                        let p = self.nodes.get_mut(&peer).unwrap();
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
            Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

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
        let leader_hwm = match self.nodes[&leader].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&leader].high_watermark,
        };
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    fn send(&mut self, src: NodeId, dst: NodeId, event: Event) {
        if self.partitioned.contains(&src) || self.partitioned.contains(&dst) {
            return;
        }
        self.queue.push_back(Message { src, dst, event });
    }

    fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Drain and deliver the queued messages back-to-front, a deliberately
    /// non-FIFO but deterministic order.
    ///
    /// This method returns the number of messages delivered.
    /// `out_of_order_delivery` calls it to show that the log stays consistent
    /// under reordered delivery.
    pub(super) fn deliver_queue_reversed(&mut self) -> usize {
        let mut drained: Vec<Message> = self.queue.drain(..).collect();
        drained.reverse();
        let n = drained.len();
        for msg in drained {
            self.deliver(&msg);
        }
        n
    }

    /// Deliver the front-of-queue message twice.
    ///
    /// The duplicate is a no-op on the recipient because `KRaft` messages carry
    /// monotonic epochs and offsets. This method returns `true` if a message
    /// was available to duplicate.
    pub(super) fn deliver_front_twice(&mut self) -> bool {
        let Some(msg) = self.queue.front().cloned() else {
            return false;
        };
        // Deliver the genuine copy.
        let first = self.queue.pop_front().expect("front exists");
        self.deliver(&first);
        // Deliver the duplicate of the same message.
        self.deliver(&msg);
        true
    }
}
