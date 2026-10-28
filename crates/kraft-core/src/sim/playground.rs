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
        rstest::fixture,
        rstest_bdd_macros::{given, scenarios, then, when},
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

    #[derive(Default)]
    struct Playground {
        sim: Option<Sim>,
        initial: Option<SimSnapshot>,
        old_leader: Option<NodeId>,
        queued_before: usize,
        steps_before: usize,
        dropped: Option<bool>,
        reordered: Option<usize>,
        duplicated: Option<bool>,
        appended: Option<bool>,
        log_before: usize,
        log_after: usize,
    }

    impl Playground {
        fn sim(&mut self) -> &mut Sim { self.sim.as_mut().expect("a playground cluster was given") }

        fn observed(&self) -> &Sim { self.sim.as_ref().expect("a playground cluster was given") }
    }

    #[fixture]
    fn playground() -> Playground {
        // Start this scenario with fresh inputs and observations.

        Playground {
            sim: None,
            ..Playground::default()
        }
    }

    #[given("a fresh three-voter playground cluster")]
    fn fresh_cluster(playground: &mut Playground) {
        let sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        playground.initial = Some(sim.snapshot());
        playground.sim = Some(sim);
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
        playground.old_leader = Some(playground.observed().leaders()[0]);
    }

    #[then("the fresh cluster had three voters and no leader")]
    fn initial_cluster(playground: &Playground) {
        let initial = playground
            .initial
            .as_ref()
            .expect("initial state was captured");
        assert!((initial.leaders.is_empty(), initial.nodes.len()) == (true, 3));
    }

    #[then("the playground reports one leader and an advancing event timeline")]
    fn stable_snapshot(playground: &Playground) {
        let sim = playground.observed();
        assert!(sim.leaders().len() == 1);
        let snap = sim.snapshot();
        check!((snap.leaders.len(), snap.clock_ms > 0, snap.step_count > 0) == (1, true, true));
    }

    #[when("the operator isolates the playground leader and heals the partition")]
    fn partition_then_heal(playground: &mut Playground) {
        let old = playground.old_leader.expect("the playground had a leader");
        playground.sim().partition(old);
        step_until(playground.sim(), 10_000, |sim| {
            sim.leaders().iter().any(|&leader| leader != old)
        });
        playground.sim().run_until_stable(10_000);
        playground.sim().heal(old);
        playground.sim().run_until_stable(10_000);
    }

    #[then("the healed playground has exactly one leader")]
    fn healed_leader(playground: &Playground) {
        assert!(playground.observed().leaders().len() == 1);
    }

    #[given("election traffic is queued in the playground")]
    fn queued_election(playground: &mut Playground) {
        fresh_cluster(playground);
        while playground.observed().in_flight().is_empty() && playground.sim().step_once() {}
        playground.queued_before = playground.observed().in_flight().len();
        playground.steps_before = playground.observed().steps().len();
    }

    #[when("the operator drops the next playground message")]
    fn drop_message(playground: &mut Playground) {
        playground.dropped = Some(playground.sim().drop_next());
    }

    #[then("one queued message is removed and one drop event is recorded")]
    fn recorded_drop(playground: &Playground) {
        let sim = playground.observed();
        assert!(playground.queued_before > 0);
        check!(playground.dropped == Some(true));
        check!(sim.in_flight().len() == playground.queued_before - 1);
        check!(sim.steps().len() == playground.steps_before + 1);
        let last = sim.steps().last().expect("drop action was recorded");
        assert!(matches!(last.action, TraceAction::Drop { .. }));
    }

    #[when("the operator reorders election traffic and duplicates the next available message")]
    fn replay_faults(playground: &mut Playground) {
        playground.reordered = Some(playground.sim().reorder());
        playground.sim().run_until_stable(10_000);
        while playground.observed().in_flight().is_empty() && playground.sim().step_once() {}
        if !playground.observed().in_flight().is_empty() {
            playground.duplicated = Some(playground.sim().duplicate_next());
        }
    }

    #[then("playground accessors and replay faults report the same event history")]
    fn consistent_faults(playground: &Playground) {
        let sim = playground.observed();
        let initial = playground
            .initial
            .as_ref()
            .expect("initial state was captured");
        assert!(sim.voter_ids() == vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert!(initial.clock_ms == 0);
        assert!(playground.queued_before > 0);
        assert!(playground.reordered.is_some_and(|count| count >= 1));
        assert!(sim.clock_ms() > 0);
        if let Some(duplicated) = playground.duplicated {
            assert!(duplicated);
        }
    }

    #[when("the operator appends two records without a playground leader")]
    fn append_without_leader(playground: &mut Playground) {
        playground.appended = Some(playground.sim().append(2));
    }

    #[then("the playground refuses the append without a leader")]
    fn append_refused(playground: &Playground) {
        assert!(playground.appended == Some(false));
    }

    #[when("the operator elects a leader and appends two records")]
    fn append_to_leader(playground: &mut Playground) {
        elect_leader(playground);
        let leader = playground.observed().leaders()[0];
        playground.log_before = log_len(playground.observed(), leader);
        playground.appended = Some(playground.sim().append(2));
        playground.log_after = log_len(playground.observed(), leader);
    }

    #[then("the elected playground leader gains exactly two records")]
    fn appended_records(playground: &Playground) {
        assert!(playground.appended == Some(true));
        assert!(playground.log_after == playground.log_before + 2);
    }

    scenarios!("tests/features/migrated/src/sim/playground.feature", fixtures = [playground: Playground]);
}
