//! Exhaustive stateright checks of the `KRaft` consensus core. See
//! `model/mod.rs`.
//!
//! Memory bound: the stateright BFS keeps every visited unique state resident.
//! Each checker run is therefore fenced with a `target_state_count` backstop,
//! on top of the model's tight `within_boundary`, so a runaway space cannot
//! exhaust the host RAM.
//!
//! The configs differ in their bounds because the linearizability tester keeps
//! its history in the fingerprinted state and so blows the space up by about
//! 30x wherever client appends are enabled:
//! - `three_voters_election_safety`: 3 voters and NO client appends. This covers
//!   election and log-matching safety over the small, fast space.
//! - `two_voters_linearizable`: 2 voters with client appends. This covers
//!   committed-log linearizability over a tightly-bounded space.
//! - `three_voters_faults`: 3 voters, no appends, message loss and duplication,
//!   and one crash at a time.
//! - `three_voters_append`: 3 voters WITH client appends and one crash at a
//!   time, which is the only config in which a committed entry can rest on a
//!   bare majority while the third voter falls behind. That is what makes the
//!   KIP-595 log-recency test in `handle_vote_request` observable: without it a
//!   stale voter wins an election and `leader_completeness` fails.
//! - `two_voters_append_via_linearizable`: 2 voters and stateless appenders.
#![cfg(test)]
mod model;

use {
    krabka_ids::NodeId,
    model::ConsensusModel,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    stateright::{Checker, Model},
};

/// Hard backstop on the explored, that is generated, states. It bounds memory
/// even if `within_boundary` is looser than intended. It is set well above the
/// true bounded count of each config, so it never truncates a real check. Such
/// a truncation would spuriously fail a `sometimes` witness, or leave an
/// `always` only partially verified.
const MAX_STATES: usize = 26_000_000;
/// Depth backstop. It must exceed the reachable-graph diameter of each config,
/// or the search is depth-truncated and therefore incomplete. The configs below
/// are bounded, so their diameter sits well under this value.
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// Two configs grew when the model began to record, for one transition, that a
// vote was refused for log recency alone (`three_voters_append` only) and that
// the production `TruncateTo` cut a divergent log (both crash configs): 779,078
// and 445,169 before. Replicating only onto a log the leader's extends, rather
// than overwriting any shorter log, left every count unchanged: within these
// bounds no follower is ever shorter than the leader and divergent when it
// fetches, so the overwrite had never fired.
//
// The same two shrank, from 779,582 and 457,697, when the production
// `handle_fetch_response` began to fetch again right after it truncates on a
// diverging epoch, as Kafka's follower does. The truncating step now also
// sends that fetch, which the model's replication abstraction answers at once:
// the truncated log takes the leader's records in the same transition, and the
// fetch replaces the node's in-flight one. The states in which a truncated
// voter sat with no fetch in flight, able to move on only through a timeout,
// are gone, and so are the states reachable only through them. The step
// cannot leave the in-flight bound: it consumes the response and adds at most
// one fetch.
//
// Every count moved again when vote and pre-vote began to follow
// `KafkaRaftClient` (#1243): a higher-epoch pre-vote now advances the epoch
// before the voter-key check, and a follower grants a pre-vote only once it has
// stopped fetching from its leader. The election-safety config lost states (a
// follower no longer grants while its leader is live) and the others gained the
// epoch advance. The `always` properties are unchanged and still hold.
//
// Three counts moved again, and the generated count of `three_voters_faults`
// passed the old 6M cap, when a replica that follows the leader of its epoch
// began to keep the vote it cast in that epoch, as Kafka's
// `QuorumState.transitionToFollower` does, and a `Prospective` replica began to
// remember the leader it abandoned. A replica that votes for one candidate and
// then follows another used to forget the vote, and so it could vote twice in
// one epoch after a fetch timeout. Every state in which a follower still holds
// its vote is new, so the three-voter configs grew by 40 to 80 percent (the
// cap rose from 6M to 8M with them). The two-voter configs never split a vote
// from a leader, and did not move. The `always` properties, election safety
// among them, still hold.
//
// Every count moved again when a nominee fenced by a newer epoch in a Vote
// response began to keep its election deadline, as Kafka's
// `QuorumState.transitionToUnattached` keeps a candidate's remaining timeout
// (#621). It used to arm its election timer at the instant of the response,
// so the states in which it starts its next pre-vote at once are gone, and a
// fenced nominee now carries the deadline it drew into the new epoch.
//
// Every count grew, and the generated count of `three_voters_faults` passed
// the old 8M cap (the cap rose to 18M with it), when a `Prospective` began to
// record the voters that refuse its pre-vote and to end a round it can no
// longer win, as Kafka's `EpochElection.isVoteRejected` and
// `prospectiveTransitionAfterElectionLoss` do (#621). The refusals are new
// state, and a lost round now leads back to the abandoned leader or to
// Unattached instead of waiting for the election timer. The `always`
// properties, election safety among them, still hold.
//
// Every count grew again, and the generated count of `three_voters_faults`
// passed the 18M cap (the cap rose to 26M with it), when a `Prospective` whose
// round timed out began to follow the leader it abandoned again, as Kafka's
// `pollProspective` does through `prospectiveTransitionAfterElectionLoss`
// (#621). A voter now cycles between following that leader and pre-voting
// instead of pre-voting round after round, and each cycle is new state.
// The same change let `three_voters_append` find a `leader_completeness`
// counterexample: the model hands every Fetch answer to the core, and
// `handle_fetch_response` fetched again even after the replica had voted in a
// newer epoch, so a stale answer from the old leader replicated an entry the
// new leader lacked. The core now acts only on an answer from the leader it
// follows, in its epoch, as the engine's fence and Kafka's
// `maybeHandleCommonResponse` do, and the `always` properties hold again.
struct ModelScenario {
    model: Option<ConsensusModel>,
    checker: Option<Box<dyn Checker<ConsensusModel>>>,
}

#[fixture]
fn consensus_model() -> ModelScenario {
    // Start this scenario with fresh inputs and observations.

    ModelScenario {
        model: None,
        checker: None,
    }
}

#[given("the bounded consensus configuration {configuration}")]
fn model_configuration(consensus_model: &mut ModelScenario, configuration: String) {
    consensus_model.model = Some(match configuration.as_str() {
        "three-voter-election-safety" => {
            ConsensusModel::elections(&[NodeId(1), NodeId(2), NodeId(3)])
        }
        "two-voter-linearizable-two-appends" => {
            ConsensusModel::linearizable(&[NodeId(1), NodeId(2)], 2)
        }
        "three-voter-loss-duplication-one-crash" => {
            ConsensusModel::faults(&[NodeId(1), NodeId(2), NodeId(3)])
        }
        "three-voter-one-append-one-crash" => {
            ConsensusModel::three_voters_append(&[NodeId(1), NodeId(2), NodeId(3)], 1)
        }
        "two-voter-stateless-two-appends" => ConsensusModel::append_via(&[NodeId(1), NodeId(2)], 2),
        _ => panic!("unknown bounded consensus configuration: {configuration}"),
    });
}

#[when("every reachable consensus state is explored breadth first")]
fn exhaust_consensus_model(consensus_model: &mut ModelScenario) {
    let model = consensus_model
        .model
        .take()
        .expect("a bounded model configuration was given");
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    consensus_model.checker = Some(Box::new(checker));
}

#[then("the model search reaches neither its state-count nor its depth backstop")]
fn exhaustive_search(consensus_model: &ModelScenario) {
    let checker = consensus_model
        .checker
        .as_ref()
        .expect("the model was explored");
    assert2::assert!(
        checker.max_depth() < MAX_DEPTH,
        "hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < MAX_STATES,
        "hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
}

#[then("the exact reachable-state count is {states}")]
fn pinned_state_count(consensus_model: &ModelScenario, states: usize) {
    let checker = consensus_model
        .checker
        .as_ref()
        .expect("the model was explored");
    assert2::assert!(
        checker.unique_state_count() == states,
        "unique-state count moved: the reachable set of this model changed"
    );
}

#[then("all consensus safety properties and progress witnesses hold")]
fn model_properties(consensus_model: &ModelScenario) {
    consensus_model
        .checker
        .as_ref()
        .expect("the model was explored")
        .assert_properties();
}

scenarios!("tests/features/migrated/tests/kraft_model.feature", fixtures = [consensus_model: ModelScenario]);
