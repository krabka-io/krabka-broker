//! Unit tests for the timer derivation in [`super::timing`]: the staggered
//! startup election deadline, the heartbeat period, and the role predicates
//! that decide which timer runs and when leadership is lost.

use assert2::{assert, check};
use krabka_units::prelude::{TimeExt as _, millis, secs};

use super::*;
use crate::kraft::controller::{
    test_support::voter_set,
    timing::{
        election_timeout_ms, election_timer_starts_election, following_leader_for_role,
        heartbeat_period, initial_election_at, instant_from_clock_base,
        should_fail_waiters_on_leadership_change,
    },
};

#[test]
fn initial_election_deadline_matches_startup_role() {
    /// Base election timeout the staggered startup deadline is derived from.
    const TIMEOUT: Time = millis(400);
    /// The same extent in the integer milliseconds the core's jitter uses.
    const TIMEOUT_MS: u64 = 400;

    let base = Instant::now();
    let single = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(&[NodeId(1)])),
        TIMEOUT,
    );
    assert2::assert!(initial_election_at(&single, None, base, NodeId(1), 0, TIMEOUT) == Some(base));

    let known_leader = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    assert2::assert!(
        initial_election_at(&known_leader, Some(NodeId(2)), base, NodeId(1), 0, TIMEOUT).is_none()
    );

    let non_voter = QuorumStateMachine::new(
        NodeId(4),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    assert2::assert!(initial_election_at(&non_voter, None, base, NodeId(4), 0, TIMEOUT).is_none());

    let multi = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    // The jitter is integer milliseconds and the deadline is the integer
    // sum, so the quantity must not shift the deadline by even a nanosecond.
    let jitter = crate::kraft::core::election_jitter_ms(NodeId(1), 0, TIMEOUT_MS);
    let at =
        initial_election_at(&multi, None, base, NodeId(1), 0, TIMEOUT).expect("multi voter timer");
    assert2::assert!(at.duration_since(base) == Duration::from_millis(TIMEOUT_MS + jitter));
}

#[test]
fn election_timeout_converts_to_whole_milliseconds() {
    for (_case, timeout, want_ms) in [
        ("whole second", secs(1), 1_000u64),
        ("sub-second", millis(250), 250),
        ("zero", secs(0), 0),
        ("negative clamps to zero", Time::from_millis(-4), 0),
    ] {
        check!(election_timeout_ms(timeout) == want_ms);
    }
}

#[test]
fn heartbeat_period_is_one_third_of_election_timeout_with_floor() {
    for (_case, timeout_ms, want_ms) in [
        ("ordinary timeout", 1000, 333),
        ("short timeout", 120, 40),
        ("floor below three milliseconds", 2, 1),
        ("zero timeout floor", 0, 1),
    ] {
        assert2::assert!(heartbeat_period(millis(timeout_ms), None) == millis(want_ms));
    }
}

#[test]
fn configured_heartbeat_overrides_derived_period() {
    assert2::assert!(heartbeat_period(secs(5), Some(millis(500))) == millis(500));
}

#[test]
fn election_timer_only_starts_non_leader_voters() {
    for (_case, is_voter, is_leader, want) in [
        ("non-leader voter", true, false, true),
        ("leader voter", true, true, false),
        ("non-voter follower", false, false, false),
        ("non-voter leader", false, true, false),
    ] {
        assert2::assert!(election_timer_starts_election(is_voter, is_leader) == want);
    }
}

#[test]
fn following_leader_for_role_reports_followed_leader_only() {
    for (role, want) in [
        (
            Role::Follower {
                leader_id: NodeId(7),
                fetch_deadline: SimInstant(10),
            },
            Some(NodeId(7)),
        ),
        (
            Role::Observer {
                leader_id: Some(NodeId(9)),
                fetch_deadline: SimInstant(10),
            },
            Some(NodeId(9)),
        ),
        (
            Role::Observer {
                leader_id: None,
                fetch_deadline: SimInstant(10),
            },
            None,
        ),
        (
            Role::Leader {
                replicas: std::collections::BTreeMap::new(),
                fetched_voters: std::collections::BTreeSet::new(),
                high_watermark: 0,
                epoch_start_offset: 0,
            },
            None,
        ),
    ] {
        assert2::assert!(following_leader_for_role(&role) == want);
    }
}

#[test]
fn leadership_loss_detection_handles_stepdown_and_epoch_bump() {
    for (_case, was_leader, is_leader, held_epoch, current_epoch, want) in [
        ("leader stepped down", true, false, 3, 3, true),
        ("leader epoch advanced", true, true, 3, 4, true),
        ("leadership unchanged", true, true, 3, 3, false),
        ("follower epoch advanced", false, false, 3, 4, false),
    ] {
        assert2::assert!(
            should_fail_waiters_on_leadership_change(
                was_leader,
                is_leader,
                held_epoch,
                current_epoch
            ) == want
        );
    }
}

#[test]
fn deadline_instant_offsets_from_engine_clock_base() {
    let base = Instant::now();
    let at = instant_from_clock_base(base, SimInstant(250));
    assert2::assert!(at.checked_duration_since(base) == Some(Duration::from_millis(250)));
}
