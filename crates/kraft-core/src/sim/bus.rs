//! The in-memory message bus: delivery, and the application of every [`Action`]
//! the state machine returns.
//!
//! This module holds the half of the harness that moves data. It hands one
//! [`Event`] to a node, turns each returned action into queued messages, log
//! appends, truncations, and timer arms, and it holds the two replay faults
//! that deliver the queue back-to-front or deliver one message twice.

use {
    super::{
        Sim,
        node::Message,
        trace::{TraceAction, event_label},
    },
    crate::{
        event::Event,
        role::Role,
        types::{LogView, NodeId},
    },
};

impl Sim {
    krabka_macros::simulation_step!(crate);

    krabka_macros::simulation_actions!(crate);

    krabka_macros::simulation_transport!(crate);

    pub(super) fn deliver(&mut self, msg: &Message) {
        let blocked = self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst);
        if blocked || !self.nodes.contains_key(&msg.dst) {
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

    fn advance_simulated_watermark(&mut self, id: NodeId, hwm: i64) {
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        node.high_watermark = hwm;
        let eligible_peers: Vec<_> = self
            .all_node_ids()
            .into_iter()
            .filter(|&peer| peer != id && !self.partitioned.contains(&peer))
            .collect();
        for peer in eligible_peers {
            if let Some(target) = self.nodes.get_mut(&peer) {
                target.high_watermark = hwm.min(target.log.end_offset());
            }
        }
    }

    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if !self.can_replicate(follower, leader) {
            return;
        }
        let Some(leader_node) = self.nodes.get(&leader) else {
            return;
        };
        let leader_hwm = match leader_node.machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => leader_node.high_watermark,
        };
        let Some(mut follower_node) = self.nodes.remove(&follower) else {
            return;
        };
        if let Some(leader_node) = self.nodes.get(&leader) {
            follower_node.log.replicate_from(&leader_node.log);
        }
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
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
        let Some(msg) = self.queue.pop_front() else {
            return false;
        };
        // Deliver the genuine copy.
        self.deliver(&msg);
        // Deliver the duplicate of the same message.
        self.deliver(&msg);
        true
    }
}
