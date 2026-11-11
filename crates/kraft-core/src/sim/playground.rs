//! The interactive control surface the in-browser playground drives.
//!
//! `krabka-playground` steps the same scheduler one microstep at a time,
//! injects the operator faults that drop, reorder, or duplicate a message on
//! the bus, and reads back a serializable [`SimSnapshot`] after every action.
//! The accessors here are the whole of what that UI needs.

use {
    super::{
        Sim,
        trace::{InFlight, SimSnapshot, TraceAction, TraceStep, event_label},
    },
    crate::types::NodeId,
};

impl Sim {
    /// Advance the simulation by one scheduler microstep.
    ///
    /// This method delivers the front-of-bus message if one is queued.
    /// Otherwise it fires the next-due timer. It returns `true` if there was a
    /// message or a timer.
    pub fn step_once(&mut self) -> bool {
        if let Some(msg) = self.queue.pop_front() {
            self.deliver(&msg);
            return true;
        }
        self.fire_next_timer()
    }

    /// Append `n` records on whichever node is currently the leader.
    ///
    /// This method backs the playground "produce" button. It returns `false` if
    /// there is no leader to append to.
    pub fn append(&mut self, n: usize) -> bool {
        let Some(leader) = self.leaders().first().copied() else {
            return false;
        };
        self.leader_append(leader, n);
        true
    }

    /// Discard the front-of-bus message instead of delivering it: the "drop"
    /// fault.
    ///
    /// This method returns `false` if the bus is empty. It records a
    /// [`TraceAction::Drop`], so the fault appears on the event timeline.
    pub fn drop_next(&mut self) -> bool {
        let Some(msg) = self.queue.pop_front() else {
            return false;
        };
        let label = event_label(&msg.event);
        self.record(
            TraceAction::Drop {
                src: msg.src.0,
                dst: msg.dst.0,
                event: label.clone(),
            },
            format!("N{} → N{}: {label} dropped in flight", msg.src, msg.dst),
        );
        true
    }

    /// Deliver every queued message back-to-front, that is, non-FIFO: the
    /// "reorder" fault.
    ///
    /// This method returns the number of messages delivered.
    pub fn reorder(&mut self) -> usize { self.deliver_queue_reversed() }

    /// Deliver the front-of-bus message twice: the "duplicate" fault.
    ///
    /// This method returns `false` if the bus is empty.
    pub fn duplicate_next(&mut self) -> bool { self.deliver_front_twice() }

    /// The current logical clock, in milliseconds.
    #[must_use]
    pub fn clock_ms(&self) -> u64 { self.now.0 }

    /// The voter ids of this cluster, ascending.
    #[must_use]
    pub fn voter_ids(&self) -> Vec<NodeId> { self.voter_ids.clone() }

    /// The recorded event timeline: every delivery, fault, timeout, and
    /// election the simulation has taken so far.
    #[must_use]
    pub fn steps(&self) -> &[TraceStep] { &self.steps }

    /// The messages currently in flight on the bus, front (next to deliver) first.
    #[must_use]
    pub fn in_flight(&self) -> Vec<InFlight> {
        self.queue
            .iter()
            .map(|m| InFlight {
                src: m.src.0,
                dst: m.dst.0,
                event: event_label(&m.event),
            })
            .collect()
    }

    /// A full, serializable snapshot of the cluster.
    ///
    /// The snapshot holds the clock, every node's role, the in-flight bus, the
    /// current leaders, and the number of steps that elapsed. The browser UI
    /// renders this after each control action.
    #[must_use]
    pub fn snapshot(&self) -> SimSnapshot {
        SimSnapshot {
            clock_ms: self.now.0,
            nodes: self.snapshot_roles(),
            in_flight: self.in_flight(),
            leaders: self.leaders().iter().map(|id| id.0).collect(),
            step_count: self.steps.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        krabka_bdd::{Given, given, then, when},
        rstest::fixture,
    };

    fn step_until<F: Fn(&Sim) -> bool>(sim: &mut Sim, max: usize, done: F) {
        for _ in 0..max {
            if done(sim) || !sim.step_once() {
                return;
            }
        }
    }

    fn log_len(sim: &Sim, node_id: NodeId) -> usize {
        sim.snapshot()
            .nodes
            .iter()
            .find(|node| node.id == node_id.0)
            .map_or(0, |node| node.log_len)
    }

    struct Playground {
        sim: Given<Sim>,
        initial: Given<SimSnapshot>,
        old_leader: Given<(NodeId, u64)>,
        partitioned: Given<SimSnapshot>,
        queued_messages: Vec<(u64, u64, String)>,
        queued_before: usize,
        steps_before: usize,
        dropped: Given<bool>,
        reordered: Given<usize>,
        duplicated: Given<bool>,
        appended: Given<bool>,
        log_before: usize,
        log_after: usize,
    }

    impl Playground {
        fn sim(&mut self) -> &mut Sim { self.sim.get_mut() }

        fn observed(&self) -> &Sim { self.sim.get() }
    }

    #[fixture]
    fn playground() -> Playground {
        Playground {
            sim: Given::new("cluster", "Given a fresh three-voter playground cluster"),
            initial: Given::new(
                "initial state",
                "Given a fresh three-voter playground cluster",
            ),
            old_leader: Given::new(
                "initial leader",
                "Given the playground has elected a leader",
            ),
            partitioned: Given::new(
                "partitioned state",
                "When the operator isolates the playground leader",
            ),
            queued_messages: Vec::new(),
            queued_before: 0,
            steps_before: 0,
            dropped: Given::new(
                "dropped message",
                "When the operator drops the next playground message",
            ),
            reordered: Given::new(
                "reordered messages",
                "When the operator reorders the queued election traffic",
            ),
            duplicated: Given::new(
                "duplicated message",
                "When the operator duplicates the next playground message",
            ),
            appended: Given::new("append result", "When the operator appends two records"),
            log_before: 0,
            log_after: 0,
        }
    }

    #[given("a fresh three-voter playground cluster")]
    fn fresh_cluster(playground: &mut Playground) {
        let sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        playground.initial.set(sim.snapshot());
        playground.sim.set(sim);
    }

    #[when("the operator steps the playground through an election")]
    fn elect_leader(playground: &mut Playground) {
        step_until(playground.sim(), 10_000, |sim| !sim.leaders().is_empty());
        playground.sim().run_until_stable(10_000);
    }

    #[given("the playground has elected a leader")]
    fn elected_cluster(playground: &mut Playground) {
        fresh_cluster(playground);
        elect_leader(playground);
        let snapshot = playground.observed().snapshot();
        let leader = snapshot
            .nodes
            .iter()
            .find(|node| node.role == "Leader")
            .expect("an elected leader");
        playground.old_leader.set((NodeId(leader.id), leader.epoch));
    }

    #[then("the fresh cluster starts leaderless with three voters")]
    fn initial_cluster(playground: &Playground) {
        let initial = playground.initial.get();
        assert!((initial.leaders.is_empty(), initial.nodes.len()) == (true, 3));
        check!(initial.clock_ms == 0);
        check!(playground.observed().voter_ids() == vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[then("the playground records the elected leader on its event timeline")]
    fn stable_snapshot(playground: &Playground) {
        let sim = playground.observed();
        assert!(sim.leaders().len() == 1);
        let snap = sim.snapshot();
        check!((snap.leaders.len(), snap.clock_ms > 0, snap.step_count > 0) == (1, true, true));
        check!(sim.clock_ms() == snap.clock_ms);
    }

    #[when("the operator isolates the playground leader")]
    fn partition_leader(playground: &mut Playground) {
        let (old, _) = *playground.old_leader.get();
        playground.sim().partition(old);
        step_until(playground.sim(), 10_000, |sim| {
            sim.leaders().iter().any(|&leader| leader != old)
        });
        playground.sim().run_until_stable(10_000);
        playground.partitioned.set(playground.observed().snapshot());
    }

    #[given("the connected majority has replaced an isolated playground leader")]
    fn replaced_leader(playground: &mut Playground) {
        elected_cluster(playground);
        partition_leader(playground);
    }

    fn replacement_leaders(playground: &Playground) -> Vec<(u64, u64)> {
        playground
            .partitioned
            .get()
            .nodes
            .iter()
            .filter(|node| !node.partitioned && node.role == "Leader")
            .map(|node| (node.id, node.epoch))
            .collect()
    }

    #[then("the connected majority elects one replacement at a higher epoch")]
    fn replacement_leader(playground: &Playground) {
        let (old, old_epoch) = *playground.old_leader.get();
        let leaders = replacement_leaders(playground);
        assert!(leaders.len() == 1);
        let (replacement, epoch) = leaders[0];
        assert!(replacement != old.0);
        assert!(epoch > old_epoch);
        let snapshot = playground.partitioned.get();
        assert!(
            snapshot
                .nodes
                .iter()
                .any(|node| node.id == old.0 && node.partitioned)
        );
    }

    #[when("the operator heals the playground partition")]
    fn heal_partition(playground: &mut Playground) {
        let (old, _) = *playground.old_leader.get();
        playground.sim().heal(old);
        playground.sim().run_until_stable(10_000);
    }

    #[then("every healed voter reaches the replacement leader's epoch")]
    fn healed_leader(playground: &Playground) {
        let leaders = replacement_leaders(playground);
        assert!(leaders.len() == 1);
        let (replacement, epoch) = leaders[0];
        let snapshot = playground.observed().snapshot();
        assert!(snapshot.leaders == vec![replacement]);
        assert!(
            snapshot
                .nodes
                .iter()
                .all(|node| !node.partitioned && node.epoch == epoch)
        );
    }

    #[given("election traffic is queued in the playground")]
    fn queued_election(playground: &mut Playground) {
        fresh_cluster(playground);
        while playground.observed().in_flight().is_empty() && playground.sim().step_once() {}
        playground.queued_before = playground.observed().in_flight().len();
        playground.queued_messages = playground
            .observed()
            .in_flight()
            .into_iter()
            .map(|message| (message.src, message.dst, message.event))
            .collect();
        playground.steps_before = playground.observed().steps().len();
    }

    #[when("the operator drops the next playground message")]
    fn drop_message(playground: &mut Playground) {
        let dropped = playground.sim().drop_next();
        playground.dropped.set(dropped);
    }

    #[then("the event timeline records the removal of one queued message")]
    fn recorded_drop(playground: &Playground) {
        let sim = playground.observed();
        assert!(playground.queued_before > 0);
        check!(*playground.dropped.get());
        check!(sim.in_flight().len() == playground.queued_before - 1);
        check!(sim.steps().len() == playground.steps_before + 1);
        let last = sim.steps().last().expect("drop action was recorded");
        assert!(matches!(last.action, TraceAction::Drop { .. }));
    }

    #[when("the operator reorders the queued election traffic")]
    fn reorder_messages(playground: &mut Playground) {
        let reordered = playground.sim().reorder();
        playground.reordered.set(reordered);
    }

    fn delivered_messages(playground: &Playground) -> Vec<(u64, u64, String)> {
        playground.observed().steps()[playground.steps_before..]
            .iter()
            .filter_map(|step| match &step.action {
                TraceAction::Deliver { src, dst, event } => Some((*src, *dst, event.clone())),
                _ => None,
            })
            .collect()
    }

    #[then("the event timeline delivers every previously queued message in reverse order")]
    fn reversed_deliveries(playground: &Playground) {
        let expected: Vec<_> = playground.queued_messages.iter().rev().cloned().collect();
        assert!(
            playground.queued_before >= 2,
            "reordering needs at least two queued messages"
        );
        assert!(*playground.reordered.get() == playground.queued_before);
        assert!(delivered_messages(playground) == expected);
    }

    #[when("the operator duplicates the next playground message")]
    fn duplicate_message(playground: &mut Playground) {
        let duplicated = playground.sim().duplicate_next();
        playground.duplicated.set(duplicated);
    }

    #[then("the event timeline records exactly two deliveries of the next queued message")]
    fn duplicated_delivery(playground: &Playground) {
        let first = playground
            .queued_messages
            .first()
            .expect("election traffic was queued");
        assert!(*playground.duplicated.get());
        assert!(delivered_messages(playground) == vec![first.clone(), first.clone()]);
        let remaining: Vec<_> = playground
            .observed()
            .in_flight()
            .into_iter()
            .map(|message| (message.src, message.dst, message.event))
            .collect();
        assert!(remaining.starts_with(&playground.queued_messages[1..]));
    }

    #[when("the operator appends two records without a playground leader")]
    fn append_without_leader(playground: &mut Playground) {
        let appended = playground.sim().append(2);
        playground.appended.set(appended);
    }

    #[then("the playground refuses the append without a leader")]
    fn append_refused(playground: &Playground) {
        assert!(!*playground.appended.get());
    }

    #[when("the operator appends two records")]
    fn append_to_leader(playground: &mut Playground) {
        let leader = playground.observed().leaders()[0];
        playground.log_before = log_len(playground.observed(), leader);
        let appended = playground.sim().append(2);
        playground.appended.set(appended);
        playground.log_after = log_len(playground.observed(), leader);
    }

    #[then("the elected playground leader gains exactly two records")]
    fn appended_records(playground: &Playground) {
        assert!(*playground.appended.get());
        assert!(playground.log_after == playground.log_before + 2);
    }

    krabka_bdd::scenarios!("tests/features/migrated/src/sim/playground.feature", fixtures = [playground: Playground]);
}
