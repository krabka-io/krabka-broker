//! The KIP-996 contested-election acceptance spike: after the Krabka leader in
//! a static mixed quorum is killed, the surviving Krabka voter can only win a
//! new election if the JVM voter grants both its pre-vote and its real vote.
//!
//! It is the counterpart to the follower-join spike, and it is what the old
//! pre-vote echo shortcut broke.

use {
    crate::{
        static_quorum_harness::{
            MixedQuorum, docker_rm, jvm_controller_properties, kafka_cluster_id_string,
            start_jvm_controller,
        },
        support,
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, NodeId},
    krabka_units::secs,
    std::{
        fs,
        process::Command,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
    uuid::Uuid,
};

const CONTESTED_CONTAINER: &str = "krabka-kip996-contested";

struct ContestedElection {
    controllers: Option<[BrokerHandle; 2]>,
    survivor: Option<BrokerHandle>,
    dirs: Option<[TempDir; 2]>,
    properties: Option<TempDir>,
    old_leader: u64,
    old_term: u64,
    survivor_id: u64,
    logs: String,
}
#[rstest::fixture]
fn contested() -> ContestedElection {
    // Start this scenario with fresh inputs and observations.

    ContestedElection {
        controllers: None,
        survivor: None,
        dirs: None,
        properties: None,
        old_leader: 0,
        old_term: 0,
        survivor_id: 0,
        logs: String::new(),
    }
}
#[rstest_bdd_macros::given(
    "a caught-up static quorum of Krabka voters 1 and 2 and JVM voter 3 with cluster id {id}"
)]
async fn caught_up_quorum(contested: &mut ContestedElection, id: String) {
    support::init_tracing();
    docker_rm(CONTESTED_CONTAINER);

    let cluster_id = Uuid::parse_str(&id).expect("cluster UUID");
    let cid_str = kafka_cluster_id_string(cluster_id);

    let ([p1, p2, p3], [c1, c2], dirs) = MixedQuorum::start(cluster_id, Some(secs(2))).await;

    // JVM voter id 3: release the dead leader fast, self-nominate slowly.
    let props = jvm_controller_properties(
        [p1, p2, p3],
        "controller.quorum.fetch.timeout.ms=300\ncontroller.quorum.election.timeout.ms=10000\n",
    );
    let propdir = start_jvm_controller(CONTESTED_CONTAINER, p3, &cid_str, &props);

    // ── Phase 1: a Krabka node leads and the JVM joins as a follower. ───────
    let deadline = Instant::now() + Duration::from_secs(50);
    let mut leader0: Option<u64> = None;
    while Instant::now() < deadline {
        let l1 = c1.controller_leader_id();
        let l2 = c2.controller_leader_id();
        if l1.is_some() && l1 == l2 && matches!(l1, Some(NodeId(1 | 2))) {
            leader0 = l1.map(|n| n.0);
            break;
        }
        // intentional: cross-checks BOTH in-process voters' leader watches for
        // agreement in {1,2}; no single awaiter expresses the l1 == l2
        // convergence, and awaiting each handle separately would drop this
        // retry and could spuriously trip on a transient election disagreement.
        time::sleep(Duration::from_millis(500)).await;
    }
    let leader0 = leader0.expect("Krabka 2/3 majority did not elect a leader in {1,2}");
    let epoch0 = c1.controller_quorum_state_for_test().current_term;
    eprintln!("phase 1: Krabka leader={leader0} epoch={epoch0}");

    // ── Phase 1b: WAIT for the JVM voter to actually join AND catch up. ──────
    // The two Krabka nodes agree on a leader in ~1-2s, but the JVM container
    // takes ~20-40s to boot and replicate. If we kill the leader before the JVM
    // is a functional, caught-up voter, the lone survivor (1 of 3) has no
    // reachable majority and stays stuck forever. So gate the kill on the JVM
    // log showing BOTH a role transition (Follower/Leader) AND high-water-mark
    // catch-up — the same join signals the sibling `static_mixed_jvm_krabka_quorum`
    // test relies on. Generous deadline to tolerate a slow JVM boot.
    let join_deadline = Instant::now() + Duration::from_secs(70);
    let mut jvm_joined = false;
    let mut last_jvm_log = String::new();
    while Instant::now() < join_deadline {
        let logs = Command::new("docker")
            .args(["logs", CONTESTED_CONTAINER])
            .output()
            .expect("docker logs");
        last_jvm_log = format!(
            "{}{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        let transitioned = last_jvm_log.contains("Completed transition to FollowerState")
            || last_jvm_log.contains("Completed transition to LeaderState");
        let caught_up =
            last_jvm_log.contains("finished catching up to the current high water mark");
        if transitioned && caught_up {
            jvm_joined = true;
            break;
        }
        // intentional: polls the external JVM container's `docker logs` for its
        // Follower/Leader transition + HWM catch-up; no in-process krabka
        // metric reflects the JVM's internal role/replication state.
        time::sleep(Duration::from_secs(1)).await;
    }
    if !jvm_joined {
        eprintln!("==== JVM controller logs (tail) — JVM NEVER JOINED ====");
        support::print_log_tail(&last_jvm_log, 40);
        let _ = fs::write("/tmp/jvm_contested.log", &last_jvm_log);
        docker_rm(CONTESTED_CONTAINER);
        // Best-effort cleanup of the in-process brokers; the process is dying.
        c1.shutdown().await;
        c2.shutdown().await;
        panic!(
            "JVM voter id 3 did not join the quorum (no Follower/Leader transition + HWM \
             catch-up) within 70s — this is a pre-existing JVM-join problem, not the \
             KIP-996 pre-vote fix. See /tmp/jvm_contested.log."
        );
    }
    eprintln!("phase 1b: JVM voter joined and caught up to HWM — safe to kill the leader");

    // ── Phase 1c: let the JVM settle into a STEADY live-fetch relationship. ──
    // The Phase 1b gate trips the instant the JVM logs both "transition to
    // FollowerState" and "finished catching up to the current high water mark"
    // — but the JVM catches up from the *bootstrap snapshot* within tens of
    // milliseconds of booting, long before it has completed a single live Fetch
    // round-trip to the leader. Killing at that instant leaves the JVM with no
    // recent successful fetch, so its FollowerState fetch-timeout clock has no
    // live baseline and (with the leader endpoint in NetworkClient connection-
    // backoff) KRaft 4.0 never promotes it to Prospective — it stays
    // Follower(leader=1) and rejects every pre-vote for the whole window.
    //
    // Sleeping here lets the JVM run several live Fetch cycles before the kill.
    // NOTE: doing so surfaced a SEPARATE, deeper Krabka blocker — the JVM
    // replicates past the bootstrap snapshot and fatal-faults applying a
    // DUPLICATE `__consumer_offsets` TopicRecord with a mismatched topic id
    // ("Found duplicate TopicRecord for __consumer_offsets with a different ID
    // than before"). That duplicate comes from both Krabka voters racing the
    // read-then-write topic-bootstrap in coordinator/bootstrap.rs, each
    // submitting a TopicRecord with its own fresh Uuid::new_v4(). Until that
    // bootstrap is made idempotent on topic id, a JVM follower that replicates
    // far enough will crash and can never grant the survivor's pre-vote.
    time::sleep(Duration::from_secs(6)).await;
    eprintln!("phase 1c: JVM has had 6s of steady fetching — killing the leader now");

    contested.controllers = Some([c1, c2]);
    contested.dirs = Some(dirs);
    contested.properties = Some(propdir);
    contested.old_leader = leader0;
    contested.old_term = epoch0;
}
#[rstest_bdd_macros::when(
    "the Krabka leader shuts down and the remaining voters have one minute to elect"
)]
async fn lose_leader(contested: &mut ContestedElection) {
    let [c1, c2] = contested.controllers.take().expect("two controllers");
    let (killed, survivor, survivor_id) = if contested.old_leader == 1 {
        (c1, c2, 2)
    } else {
        (c2, c1, 1)
    };
    killed.shutdown().await;
    contested.survivor_id = survivor_id;
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut tick = 0u32;
    while Instant::now() < deadline {
        let state = survivor.controller_quorum_state_for_test();
        if tick.is_multiple_of(4) {
            eprintln!(
                "survivor {survivor_id}: leader={:?} term={} previous={}",
                state.current_leader, state.current_term, contested.old_term
            );
        }
        if state.current_leader == Some(NodeId(survivor_id))
            && state.current_term > contested.old_term
        {
            break;
        }
        tick += 1;
        time::sleep(Duration::from_millis(500)).await;
    }
    contested.survivor = Some(survivor);
    contested.logs = support::save_jvm_logs(CONTESTED_CONTAINER, "/tmp/jvm_contested.log");
    support::print_log_tail(&contested.logs, 40);
}
#[rstest_bdd_macros::then(
    "the surviving Krabka voter is leader at a higher term and the JVM reports no fatal fault"
)]
fn recovered_leader(contested: &ContestedElection) {
    let state = contested
        .survivor
        .as_ref()
        .expect("surviving controller")
        .controller_quorum_state_for_test();
    assert!(
        state.current_leader == Some(NodeId(contested.survivor_id))
            && state.current_term > contested.old_term,
        "survivor leader={:?} term={} previous={}",
        state.current_leader,
        state.current_term,
        contested.old_term
    );
    assert!(
        !contested.logs.contains("Encountered fatal fault"),
        "JVM controller fatal fault; see /tmp/jvm_contested.log"
    );
}

/// KIP-996 CONTESTED-ELECTION ACCEPTANCE TEST, Docker-gated and `#[ignore]`.
///
/// Two Krabka voters, ids 1 and 2, and one `mirror.gcr.io/apache/kafka:4.0.0`
/// voter, id 3, form a static 3-voter quorum. After the test kills the Krabka
/// leader, only one Krabka voter and the JVM voter survive. The surviving
/// Krabka candidate can then reach a majority only if the JVM grants both its
/// PRE-VOTE and its real vote.
///
/// The old `PRE_VOTE_ECHO_TAG` shortcut broke this path, because it dropped a
/// JVM pre-vote grant.
///
/// The JVM is tuned to release the dead leader quickly but to self-nominate
/// slowly, so the surviving Krabka node wins. Recovery to a new Krabka leader
/// at a higher epoch is the proof.
///
/// Run:
/// ```text
/// cargo test -p krabka-broker --test jvm_static_quorum_spike \
///   contested_election_krabka_counts_jvm_prevote -- --ignored --nocapture
/// ```
#[ignore = "requires Docker + a published controller port"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_static_quorum_spike/contested_election.feature",
    name = "contested election krabka counts jvm prevote"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contested_election_krabka_counts_jvm_prevote(contested: ContestedElection) {
    docker_rm(CONTESTED_CONTAINER);
    contested
        .survivor
        .take()
        .expect("survivor")
        .shutdown()
        .await;
    drop(contested);
}
