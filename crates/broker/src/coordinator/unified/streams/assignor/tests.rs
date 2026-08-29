//! Behaviour tests for [`assign`](super::assign), covering stickiness,
//! balancing, standby placement, warmup deferral, and `Auto` resolution.

use assert2::{assert, check};

use super::*;

fn member(id: &str, process: &str) -> AssignorMember {
    AssignorMember {
        member_id: id.to_owned(),
        process_id: process.to_owned(),
        rack_id: None,
        current_active: BTreeMap::new(),
        current_standby: BTreeMap::new(),
        current_warmup: BTreeMap::new(),
        task_lag: BTreeMap::new(),
    }
}

fn input(tasks: &[(&str, &[i32])], stateful: &[&str], kind: StreamsAssignorKind) -> AssignorInput {
    AssignorInput {
        tasks: tasks
            .iter()
            .map(|(s, p)| ((*s).to_owned(), p.to_vec()))
            .collect(),
        stateful: stateful.iter().map(|s| (*s).to_owned()).collect(),
        num_standby_replicas: 0,
        num_warmup_replicas: 0,
        acceptable_recovery_lag: 10_000,
        kind,
    }
}

/// Returns the total number of active tasks across all members in an
/// assignment.
fn count(role: &HashMap<String, BTreeMap<String, Vec<i32>>>) -> usize {
    role.values().flat_map(BTreeMap::values).map(Vec::len).sum()
}

#[test]
fn empty_members_empty_assignment() {
    let inp = input(&[("a", &[0, 1])], &[], StreamsAssignorKind::Sticky);
    let out = assign(&[], &inp);
    assert!(
        out == StreamsAssignment {
            active: HashMap::new(),
            standby: HashMap::new(),
            warmup: HashMap::new(),
        }
    );
}

#[test]
fn single_member_single_stateless_subtopology() {
    let members = [member("A", "p1")];
    let inp = input(&[("sub-0", &[0, 1, 2])], &[], StreamsAssignorKind::Sticky);
    let out = assign(&members, &inp);
    assert!(
        out == StreamsAssignment {
            active: HashMap::from([(
                "A".to_string(),
                BTreeMap::from([("sub-0".to_string(), vec![0, 1, 2])]),
            )]),
            standby: HashMap::new(),
            warmup: HashMap::new(),
        }
    );
}

#[test]
fn two_members_four_stateless_tasks_balanced() {
    let members = [member("A", "p1"), member("B", "p2")];
    let inp = input(
        &[("sub-0", &[0, 1, 2, 3])],
        &[],
        StreamsAssignorKind::Sticky,
    );
    let out = assign(&members, &inp);
    // 2/2 balanced; deterministic: least-loaded fills A first, then B,
    // alternating.
    assert!(
        out.active
            == HashMap::from([
                (
                    "A".to_string(),
                    BTreeMap::from([("sub-0".to_string(), vec![0, 2])]),
                ),
                (
                    "B".to_string(),
                    BTreeMap::from([("sub-0".to_string(), vec![1, 3])]),
                ),
            ])
    );
    // Re-running yields identical output.
    let out2 = assign(&members, &inp);
    assert!(out.active == out2.active);
}

#[test]
fn stickiness_keeps_owned_tasks() {
    let mut a = member("A", "p1");
    a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
    let b = member("B", "p2");
    let members = [a, b];
    // Universe grew to 4 partitions; A owns 0,1 already.
    let inp = input(
        &[("sub-0", &[0, 1, 2, 3])],
        &[],
        StreamsAssignorKind::Sticky,
    );
    let out = assign(&members, &inp);
    // A keeps 0,1 (sticky); B fills the rest to balance 2/2.
    assert!(out.active["A"]["sub-0"] == vec![0, 1]);
    assert!(out.active["B"]["sub-0"] == vec![2, 3]);
}

#[test]
fn stickiness_rebalances_when_skewed() {
    // A owns all 4; adding B must move some over to balance.
    let mut a = member("A", "p1");
    a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1, 2, 3])]);
    let b = member("B", "p2");
    let members = [a, b];
    let inp = input(
        &[("sub-0", &[0, 1, 2, 3])],
        &[],
        StreamsAssignorKind::Sticky,
    );
    let out = assign(&members, &inp);
    assert!(out.active["A"]["sub-0"].len() == 2);
    assert!(out.active["B"]["sub-0"].len() == 2);
}

#[test]
fn highly_available_standby_on_other_process() {
    let members = [member("A", "p1"), member("B", "p2")];
    let mut inp = input(
        &[("sub-0", &[0, 1])],
        &["sub-0"],
        StreamsAssignorKind::HighlyAvailable,
    );
    inp.num_standby_replicas = 1;
    let out = assign(&members, &inp);
    // Each of the 2 active tasks gets exactly one standby.
    assert!(count(&out.active) == 2);
    assert!(count(&out.standby) == 2);
    // The standby for a task sits on the *other* process than its active.
    for (sub, parts) in [("sub-0", vec![0, 1])]
        .iter()
        .flat_map(|(s, ps)| ps.iter().map(move |p| ((*s).to_owned(), *p)))
    {
        let active_owner = out
            .active
            .iter()
            .find(|(_, m)| m.get(&sub).is_some_and(|v| v.contains(&parts)))
            .map(|(id, _)| id.clone())
            .expect("active owner exists");
        let standby_owner = out
            .standby
            .iter()
            .find(|(_, m)| m.get(&sub).is_some_and(|v| v.contains(&parts)))
            .map(|(id, _)| id.clone())
            .expect("standby owner exists");
        assert!(active_owner != standby_owner);
    }
}

#[test]
fn highly_available_same_process_no_standby() {
    // Both members in the same process: no fault-tolerant standby possible.
    let members = [member("A", "p1"), member("B", "p1")];
    let mut inp = input(
        &[("sub-0", &[0, 1])],
        &["sub-0"],
        StreamsAssignorKind::HighlyAvailable,
    );
    inp.num_standby_replicas = 1;
    let out = assign(&members, &inp);
    assert!(count(&out.active) == 2);
    assert!(count(&out.standby) == 0);
}

#[test]
fn warmup_deferral_when_target_not_caught_up() {
    // A currently owns both partitions; balanced target wants to move one
    // to B. B has no lag info -> not caught up -> defer + warmup.
    let mut a = member("A", "p1");
    a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
    let b = member("B", "p2");
    let members = [a, b];
    let mut inp = input(
        &[("sub-0", &[0, 1])],
        &["sub-0"],
        StreamsAssignorKind::HighlyAvailable,
    );
    inp.num_warmup_replicas = 2;
    inp.num_standby_replicas = 0;
    let out = assign(&members, &inp);
    // Active stays on A (move deferred); B holds a warmup.
    check!(count(&out.active) == 2);
    check!(out.active["A"]["sub-0"].len() == 2);
    check!(count(&out.warmup) == 1);
    check!(out.warmup.contains_key("B"));
}

#[test]
fn warmup_promotes_when_caught_up() {
    // Same skew, but B reports lag within acceptable bounds for the task it
    // would receive -> active move applied immediately, no warmup.
    let mut a = member("A", "p1");
    a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
    let mut b = member("B", "p2");
    // The balanced target moves the lexicographically-largest task (1) off
    // A onto B; report B caught up on it.
    b.task_lag = BTreeMap::from([(("sub-0".to_owned(), 1), 5_i64)]);
    let members = [a, b];
    let mut inp = input(
        &[("sub-0", &[0, 1])],
        &["sub-0"],
        StreamsAssignorKind::HighlyAvailable,
    );
    inp.num_warmup_replicas = 2;
    inp.acceptable_recovery_lag = 10;
    let out = assign(&members, &inp);
    // Move applied: A keeps 0, B takes 1; no warmup.
    assert!(
        out.active
            == HashMap::from([
                (
                    "A".to_string(),
                    BTreeMap::from([("sub-0".to_string(), vec![0])]),
                ),
                (
                    "B".to_string(),
                    BTreeMap::from([("sub-0".to_string(), vec![1])]),
                ),
            ])
    );
    assert!(out.warmup.is_empty());
}

#[test]
fn warmup_cap_respected() {
    // Two tasks both need to move to B, but the warmup cap is 1.
    let mut a = member("A", "p1");
    a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1, 2, 3])]);
    let b = member("B", "p2");
    let members = [a, b];
    let mut inp = input(
        &[("sub-0", &[0, 1, 2, 3])],
        &["sub-0"],
        StreamsAssignorKind::HighlyAvailable,
    );
    inp.num_warmup_replicas = 1;
    inp.num_standby_replicas = 0;
    let out = assign(&members, &inp);
    // Balance wants 2 tasks on B but neither is caught up -> both deferred;
    // only one warmup created (cap = 1); both stay active on A.
    assert!(out.active["A"]["sub-0"].len() == 4);
    assert!(count(&out.warmup) == 1);
}

#[test]
fn auto_resolves_to_sticky_when_stateless() {
    let members = [member("A", "p1"), member("B", "p2")];
    let mut inp = input(&[("sub-0", &[0, 1])], &[], StreamsAssignorKind::Auto);
    inp.num_standby_replicas = 1;
    inp.num_warmup_replicas = 2;
    let out = assign(&members, &inp);
    // Sticky: active-only, no standby/warmup even with non-zero knobs.
    check!(count(&out.active) == 2);
    check!(out.standby.is_empty());
    check!(out.warmup.is_empty());
}

#[test]
fn auto_resolves_to_highly_available_when_stateful() {
    let members = [member("A", "p1"), member("B", "p2")];
    let mut inp = input(&[("sub-0", &[0, 1])], &["sub-0"], StreamsAssignorKind::Auto);
    inp.num_standby_replicas = 1;
    let out = assign(&members, &inp);
    // HighlyAvailable: stateful tasks get standby copies.
    assert!(count(&out.active) == 2);
    assert!(count(&out.standby) == 2);
}
