use {
    super::*,
    crate::{
        core::{
            election_jitter_ms,
            test_support::{
                FakeLog, MachineAndLog, check_prevote_started, machine, three_voter_machine, voters,
            },
        },
        event::{Event, LogEnd},
    },
    assert2::check,
};

/// An election at time 2000 with a fresh, up-to-date epoch-1 log.
fn start_election(ids: &[NodeId]) -> MachineAndLog {
    let mut m = machine(NodeId(1), ids);
    let log = FakeLog::new(5, 1);
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    MachineAndLog { machine: m, log }
}

/// A three-voter machine that has won its pre-vote and is now a Candidate.
fn pre_vote_promoted_candidate() -> MachineAndLog {
    let MachineAndLog {
        machine: mut m,
        log,
    } = start_election(&[NodeId(1), NodeId(2), NodeId(3)]);
    vote_response().m(&mut m).log(&log).call();
    MachineAndLog { machine: m, log }
}

fn five_voter_with_one_grant() -> MachineAndLog {
    let MachineAndLog {
        machine: mut m,
        log,
    } = start_election(&[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]);
    vote_response().m(&mut m).log(&log).call();
    MachineAndLog { machine: m, log }
}

fn prospective_three_voter() -> MachineAndLog {
    let MachineAndLog { machine, log } = start_election(&[NodeId(1), NodeId(2), NodeId(3)]);
    assert2::assert!(matches!(machine.role(), Role::Prospective { .. }));
    MachineAndLog { machine, log }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct VoteEpoch(Epoch);

#[derive(Clone, Copy, Default)]
enum VoteDecision {
    #[default]
    Granted,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum VoteRetention {
    Kept,
    Cleared,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct VoteResponseSetup {
    #[default(NodeId(2))]
    from: NodeId,
    #[default(VoteEpoch(0))]
    epoch: VoteEpoch,
    decision: VoteDecision,
    #[default(SimInstant(2001))]
    now: SimInstant,
}

#[bon::builder]
fn vote_response(
    m: &mut QuorumStateMachine,
    log: &dyn LogView,
    #[builder(into, default = NodeId (2))] from: NodeId,
    #[builder(into, default = VoteEpoch (0))] epoch: VoteEpoch,
    #[builder(default)] decision: VoteDecision,
    #[builder(into, default = SimInstant (2001))] now: SimInstant,
) -> Vec<Action> {
    let setup = VoteResponseSetup {
        from,
        epoch,
        decision,
        now,
    };
    m.on_event(
        Event::ReceiveVoteResponse {
            from: setup.from,
            epoch: setup.epoch.0,
            vote_granted: matches!(setup.decision, VoteDecision::Granted),
        },
        log,
        setup.now,
    )
}

/// Only a rejection from a *higher* epoch fences us.
///
/// Each piece of `!granted && epoch > ours` matters: a grant must never
/// fence, a rejection at our own epoch must not either -- that is the
/// ordinary "you lost the vote" reply -- and only a rejection carrying a
/// newer epoch means the cluster has moved past us. Stepping down clears
/// the vote we are holding, so whether the vote survives is the tell.
#[yare::parameterized(
    same_epoch_rejection = { VoteDecision::Rejected, VoteEpoch(3), VoteRetention::Kept },
    higher_epoch_grant = { VoteDecision::Granted, VoteEpoch(9), VoteRetention::Kept },
    higher_epoch_rejection = { VoteDecision::Rejected, VoteEpoch(9), VoteRetention::Cleared },
)]
fn only_a_rejection_from_a_higher_epoch_steps_us_down(
    decision: VoteDecision,
    epoch: VoteEpoch,
    keeps_vote: VoteRetention,
) {
    let log = FakeLog::new(5, 1);
    let mut m = three_voter_machine();
    let local_voter = NodeId(1);
    let challenger = NodeId(2);
    let binding_epoch = 3;
    let directory_id = uuid::Uuid::nil();
    // Cast a binding vote at epoch 3, so a step-down has something to
    // clear and "nothing happened" is distinguishable.
    m.on_event(
        Event::ReceiveVoteRequest {
            from: challenger,
            cluster_id: None,
            voter_id: local_voter,
            voter_directory_id: directory_id,
            candidate_epoch: binding_epoch,
            candidate: challenger,
            candidate_directory_id: directory_id,
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    check!(m.quorum_state().voted_key.is_some(), "setup should vote");

    vote_response()
        .m(&mut m)
        .log(&log)
        .epoch(epoch)
        .decision(decision)
        .now(0)
        .call();
    let kept = if m.quorum_state().voted_key.is_some() {
        VoteRetention::Kept
    } else {
        VoteRetention::Cleared
    };
    check!(kept == keeps_vote, "vote retention = {kept:?}");
}

#[test]
fn election_timeout_starts_prevote_prospective() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(5, 1);
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    check_prevote_started(&m, &actions);
    check!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
}

#[test]
/// A JVM voter's `VoteResponse` has no pre-vote flag; while Prospective, a
/// grant still counts as a pre-vote and promotes to Candidate at epoch 1.
fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = start_election(&[NodeId(1), NodeId(2), NodeId(3)]); // Prospective
    // 1 (self) + grant from 2 = majority of 3
    let actions = vote_response().m(&mut m).log(&log).call();
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.quorum_state().leader_epoch,
            m.quorum_state().voted_key.map(|k| k.id),
        ) == (true, 1, Some(NodeId(1)))
    );
    assert2::assert!(actions.iter().any(|a| matches!(a, Action::SendVoteRequest {
        pre_vote: false,
        epoch: 1
    })));
}

#[test]
fn real_majority_promotes_to_leader_and_appends_leader_change() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = pre_vote_promoted_candidate();
    let actions = vote_response()
        .m(&mut m)
        .log(&log)
        .epoch(1)
        .now(2002)
        .call();
    check!(
        (
            m.role().is_leader(),
            m.quorum_state().leader_id,
            actions
                .iter()
                .any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 })),
            actions
                .iter()
                .any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 })),
        ) == (true, Some(NodeId(1)), true, true)
    );
}

#[test]
fn observer_never_starts_election() {
    let mut m = machine(NodeId(99), &[NodeId(1), NodeId(2), NodeId(3)]); // 99 is not a voter
    let log = FakeLog::new(5, 1);
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert2::assert!(matches!(m.role(), Role::Observer { .. }));
    assert2::assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::SendVoteRequest { .. }))
    );
}

#[test]
fn stale_prevote_grant_ignored_after_promotion() {
    // A late pre-vote grant at the old epoch must not be miscounted toward
    // the real election once we have promoted to Candidate at epoch+1.
    let MachineAndLog {
        machine: mut m,
        log,
    } = pre_vote_promoted_candidate(); // → Candidate @ epoch 1
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
    // A duplicate/late pre-vote grant still tagged epoch 0 arrives.
    let actions = vote_response().m(&mut m).log(&log).from(3).now(2002).call();
    // Epoch guard (0 != 1) drops it: we stay Candidate, do NOT become leader.
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.role().is_leader(),
            actions.is_empty()
        ) == (true, false, true)
    );
    // The ignored stale grant must not have entered the real-vote tally:
    // after promotion the Candidate's grant set holds only our self-vote.
    if let Role::Candidate { granted, .. } = m.role() {
        assert2::assert!((granted.len(), granted.contains(&NodeId(3))) == (1, false));
    } else {
        panic!("expected Candidate");
    }
}

#[test]
fn late_grant_from_removed_voter_does_not_count() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = five_voter_with_one_grant();
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));

    m.apply_voter_set(voters(&[NodeId(1), NodeId(4), NodeId(5)]), SimInstant(2002));
    let actions = vote_response().m(&mut m).log(&log).now(2003).call();
    check!(
        (
            matches!(m.role(), Role::Prospective { .. }),
            actions.is_empty()
        ) == (true, true)
    );

    vote_response().m(&mut m).log(&log).from(4).now(2004).call();
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
}

#[test]
fn removed_voter_response_retallies_retained_grants() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = five_voter_with_one_grant();

    m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(4)]), SimInstant(2002));
    let actions = vote_response().m(&mut m).log(&log).from(3).now(2003).call();

    check!(
        matches!(m.role(), Role::Candidate { .. }),
        "retained grants form a majority after the voter-set shrink"
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::SendVoteRequest {
                epoch: 1,
                pre_vote: false
            }))
    );
}

#[test]
fn prospective_ignores_grant_from_different_epoch() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = prospective_three_voter();
    let actions = vote_response().m(&mut m).log(&log).epoch(5).call();
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    assert2::assert!(actions.is_empty());
}

/// The election deadline a nominee holds, whatever its round.
fn nominee_deadline(m: &QuorumStateMachine) -> SimInstant {
    match m.role() {
        Role::Prospective {
            election_deadline, ..
        }
        | Role::Candidate {
            election_deadline, ..
        } => *election_deadline,
        other => panic!("expected a nominee, found {other:?}"),
    }
}

#[derive(Clone, Copy, Debug)]
enum NomineeRound {
    PreVote,
    Vote,
}

/// A rejection from a newer epoch moves a prospective or a candidate to
/// Unattached in that epoch with the election deadline it already had, as
/// Kafka's `QuorumState.transitionToUnattached` keeps a nominee's remaining
/// election timeout.
///
/// A nominee that started its next election at once would run it at the same
/// instant as every other voter that the same epoch fenced, and two voters
/// that lost an election together would split the next vote too.
#[yare::parameterized(
    prospective = { NomineeRound::PreVote },
    candidate = { NomineeRound::Vote },
)]
fn a_nominee_fenced_by_a_newer_epoch_keeps_its_election_deadline(round: NomineeRound) {
    let MachineAndLog {
        machine: mut m,
        log,
    } = match round {
        NomineeRound::PreVote => prospective_three_voter(),
        NomineeRound::Vote => pre_vote_promoted_candidate(),
    };
    let deadline = nominee_deadline(&m);

    let actions = vote_response()
        .m(&mut m)
        .log(&log)
        .epoch(9)
        .decision(VoteDecision::Rejected)
        .now(2_100)
        .call();

    check!(
        actions
            == vec![
                Action::PersistQuorumState,
                Action::TransitionedTo("Unattached"),
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline,
                },
            ],
        "{round:?}"
    );
    check!(
        *m.role()
            == Role::Unattached {
                election_deadline: deadline,
            },
        "{round:?}"
    );
    check!(m.quorum_state().leader_epoch == 9, "{round:?}");
}

/// A candidate that has not won by its election deadline goes back to a
/// pre-vote round in the same epoch, with an election timeout drawn afresh, as
/// Kafka's `pollCandidate` calls `transitionToProspective` and
/// `QuorumState.transitionToProspective` draws `randomElectionTimeoutMs`.
#[test]
fn a_candidate_that_times_out_pre_votes_again_with_a_fresh_timeout() {
    let MachineAndLog {
        machine: mut m,
        log,
    } = pre_vote_promoted_candidate();
    let deadline = nominee_deadline(&m);

    let actions = m.on_event(Event::ElectionTimeout, &log, deadline);

    let fresh = deadline.saturating_add_ms(1_000 + election_jitter_ms(NodeId(1), 1, 1_000));
    check!(
        actions
            == vec![
                Action::TransitionedTo("Prospective"),
                Action::SendVoteRequest {
                    epoch: 1,
                    pre_vote: true,
                },
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline: fresh,
                },
            ]
    );
    check!(
        *m.role()
            == Role::Prospective {
                granted: BTreeSet::from([NodeId(1)]),
                rejected: BTreeSet::new(),
                election_deadline: fresh,
                abandoned_leader: None,
            }
    );
}

/// Every election timer a voter arms runs for Kafka's
/// `electionTimeoutMs + random.nextInt(electionTimeoutMs)`: at least the
/// configured timeout and less than twice it. The draw also differs between
/// voters, so two voters that time out together in one epoch do not keep
/// timing out together.
#[test]
fn every_election_timer_lies_in_kafkas_jittered_window() {
    let log = FakeLog::new(5, 1);
    let now = SimInstant(10_000);
    let mut deadlines = BTreeSet::new();
    for id in 1..=5 {
        for epoch in 0..64 {
            let mut m = machine(NodeId(id), &[
                NodeId(1),
                NodeId(2),
                NodeId(3),
                NodeId(4),
                NodeId(5),
            ]);
            m.force_epoch(epoch);
            m.on_event(Event::ElectionTimeout, &log, now);
            let deadline = nominee_deadline(&m);
            check!(
                (now.saturating_add_ms(1_000)..now.saturating_add_ms(2_000)).contains(&deadline),
                "node {id} epoch {epoch}: {deadline:?}"
            );
            deadlines.insert(deadline);
        }
    }
    check!(
        deadlines.len() > 64,
        "{} distinct deadlines",
        deadlines.len()
    );
}

/// How a pre-vote round began: from following a leader whose fetches stopped,
/// or with no leader to abandon.
#[derive(Clone, Copy, Debug)]
enum PreVoteStart {
    AbandonedLeader,
    NoLeader,
}

/// A Prospective at epoch 1 among `ids`, started as `start` at time 2000.
///
/// With an abandoned leader it followed voter 2 and timed out fetching from
/// it; without one it first voted for voter 3 in epoch 1, so that losing keeps
/// a vote to show.
fn prospective_at_epoch_one(ids: &[NodeId], start: PreVoteStart) -> MachineAndLog {
    let mut m = machine(NodeId(1), ids);
    let log = FakeLog::new(5, 1);
    match start {
        PreVoteStart::AbandonedLeader => {
            m.on_event(
                Event::ReceiveBeginQuorumEpoch {
                    leader_id: NodeId(2),
                    leader_epoch: 1,
                },
                &log,
                SimInstant(1_000),
            );
            m.on_event(Event::FetchTimeout, &log, SimInstant(2_000));
        }
        PreVoteStart::NoLeader => {
            m.on_event(
                Event::ReceiveVoteRequest {
                    from: NodeId(3),
                    cluster_id: None,
                    voter_id: NodeId(1),
                    voter_directory_id: uuid::Uuid::nil(),
                    candidate_epoch: 1,
                    candidate: NodeId(3),
                    candidate_directory_id: uuid::Uuid::nil(),
                    candidate_log_end: LogEnd {
                        last_epoch: 1,
                        last_offset: 5,
                    },
                    pre_vote: false,
                },
                &log,
                SimInstant(1_000),
            );
            m.on_event(Event::ElectionTimeout, &log, SimInstant(2_000));
        }
    }
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    MachineAndLog { machine: m, log }
}

/// The actions and role of a replica that, at `now`, goes back to following
/// node 2, the leader it abandoned when its pre-vote round began.
///
/// Its fetch deadline is one fetch timeout out, two seconds in the fixture.
fn following_node_two_again(now: SimInstant) -> (Vec<Action>, Role) {
    let fetch_deadline = now.saturating_add_ms(2_000);
    (
        vec![
            Action::PersistQuorumState,
            Action::TransitionedTo("Follower"),
            Action::SendFetch {
                leader_id: NodeId(2),
            },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ],
        Role::Follower {
            leader_id: NodeId(2),
            fetch_deadline,
            has_fetched_from_leader: false,
        },
    )
}

/// A Prospective whose pre-vote can no longer reach a majority ends the round
/// at once, as Kafka's `EpochElection.isVoteRejected` and
/// `prospectiveTransitionAfterElectionLoss` do. It follows the leader it
/// abandoned again, since the voters that refused it still follow that
/// leader, or becomes Unattached in the same epoch, keeping its vote, when it
/// abandoned none. A refusal that still leaves a majority possible changes
/// nothing.
#[yare::parameterized(
    three_voters_first_refusal = { 3, &[2], PreVoteStart::AbandonedLeader, None },
    three_voters_lost_to_a_leader = { 3, &[2, 3], PreVoteStart::AbandonedLeader, Some("Follower") },
    three_voters_lost_without_a_leader = { 3, &[2, 3], PreVoteStart::NoLeader, Some("Voted") },
    five_voters_two_refusals = { 5, &[2, 3], PreVoteStart::AbandonedLeader, None },
    five_voters_lost = { 5, &[2, 3, 4], PreVoteStart::AbandonedLeader, Some("Follower") },
    a_refusal_repeated = { 3, &[3, 3], PreVoteStart::NoLeader, None },
)]
fn a_prospective_that_can_no_longer_win_ends_its_round(
    voter_count: u64,
    refusers: &[u64],
    start: PreVoteStart,
    ends_as: Option<&'static str>,
) {
    let ids: Vec<NodeId> = (1..=voter_count).map(NodeId).collect();
    let MachineAndLog {
        machine: mut m,
        log,
    } = prospective_at_epoch_one(&ids, start);
    let before = m.clone();

    let mut actions = Vec::new();
    for &from in refusers {
        actions = vote_response()
            .m(&mut m)
            .log(&log)
            .from(from)
            .epoch(1)
            .decision(VoteDecision::Rejected)
            .now(2_100)
            .call();
    }

    let Some(role) = ends_as else {
        check!(actions.is_empty());
        check!(m.quorum_state() == before.quorum_state());
        return;
    };
    let now = SimInstant(2_100);
    let expected = match start {
        PreVoteStart::AbandonedLeader => {
            let (actions, role) = following_node_two_again(now);
            (actions, role, Some(NodeId(2)), None)
        }
        PreVoteStart::NoLeader => {
            let election_deadline =
                now.saturating_add_ms(1_000 + election_jitter_ms(NodeId(1), 1, 1_000));
            (
                vec![
                    Action::PersistQuorumState,
                    Action::TransitionedTo("Voted"),
                    Action::ResetTimer {
                        kind: TimerKind::Election,
                        deadline: election_deadline,
                    },
                ],
                Role::Voted { election_deadline },
                None,
                Some(NodeId(3)),
            )
        }
    };
    let (expected_actions, expected_role, leader_id, voted_for) = expected;
    check!(m.role().name() == role);
    check!(actions == expected_actions);
    check!(*m.role() == expected_role);
    check!(
        (
            m.quorum_state().leader_epoch,
            m.quorum_state().leader_id,
            m.quorum_state().voted_key.map(|key| key.id),
        ) == (1, leader_id, voted_for)
    );
}

/// Who leads after a pre-vote round timed out, neither won nor lost.
#[derive(Clone, Copy, Debug)]
enum AfterTimedOutRound {
    /// The replica follows the leader it abandoned again.
    FollowsNodeTwo,
    /// The replica starts another pre-vote round in the same epoch.
    PreVotesAgain,
}

/// A Prospective whose round timed out, neither won nor lost, ends it as
/// Kafka's `pollProspective` does, through
/// `prospectiveTransitionAfterElectionLoss`.
///
/// It follows the leader it abandoned again, and fetches from it at once.
/// Without one, it pre-votes again at once, as Kafka's replica does through
/// an Unattached state whose election timeout has already run out.
#[yare::parameterized(
    abandoned_leader = { PreVoteStart::AbandonedLeader, AfterTimedOutRound::FollowsNodeTwo },
    no_leader = { PreVoteStart::NoLeader, AfterTimedOutRound::PreVotesAgain },
)]
fn a_prospective_whose_round_times_out_ends_it(start: PreVoteStart, after: AfterTimedOutRound) {
    let MachineAndLog {
        machine: mut m,
        log,
    } = prospective_at_epoch_one(&[NodeId(1), NodeId(2), NodeId(3)], start);
    let deadline = nominee_deadline(&m);
    let voted_key = m.quorum_state().voted_key;

    let actions = m.on_event(Event::ElectionTimeout, &log, deadline);

    let (expected_actions, expected_role, leader_id) = match after {
        AfterTimedOutRound::FollowsNodeTwo => {
            let (actions, role) = following_node_two_again(deadline);
            (actions, role, Some(NodeId(2)))
        }
        AfterTimedOutRound::PreVotesAgain => {
            let election_deadline =
                deadline.saturating_add_ms(1_000 + election_jitter_ms(NodeId(1), 1, 1_000));
            (
                vec![
                    Action::TransitionedTo("Prospective"),
                    Action::SendVoteRequest {
                        epoch: 1,
                        pre_vote: true,
                    },
                    Action::ResetTimer {
                        kind: TimerKind::Election,
                        deadline: election_deadline,
                    },
                ],
                Role::Prospective {
                    granted: BTreeSet::from([NodeId(1)]),
                    rejected: BTreeSet::new(),
                    election_deadline,
                    abandoned_leader: None,
                },
                None,
            )
        }
    };
    check!(actions == expected_actions, "{after:?}");
    check!(*m.role() == expected_role, "{after:?}");
    check!(
        (
            m.quorum_state().leader_epoch,
            m.quorum_state().leader_id,
            m.quorum_state().voted_key,
        ) == (1, leader_id, voted_key),
        "{after:?}"
    );
}
