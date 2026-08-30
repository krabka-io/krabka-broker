//! The KIP-996 contested-election acceptance spike: after the Krabka leader in
//! a static mixed quorum is killed, the surviving Krabka voter can only win a
//! new election if the JVM voter grants both its pre-vote and its real vote.
//!
//! It is the counterpart to the follower-join spike, and it is what the old
//! pre-vote echo shortcut broke.

use std::{net::SocketAddr, process::Command, time::Duration};

use krabka_broker::{Broker, BrokerHandle};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    static_quorum_harness::{
        KAFKA_IMAGE, docker_rm, kafka_cluster_id_string, krabka_controller_config,
    },
    support,
};

const CONTESTED_CONTAINER: &str = "krabka-kip996-contested";

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller port"]
async fn contested_election_krabka_counts_jvm_prevote() {
    support::init_tracing();
    docker_rm(CONTESTED_CONTAINER);

    let cluster_id = Uuid::from_u128(0x4b69_7039_3936_4350_7245_566f_7445_7374);
    let cid_str = kafka_cluster_id_string(cluster_id);

    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();
    let krabka_ctrl_1: SocketAddr = format!("0.0.0.0:{p1}").parse().unwrap();
    let krabka_ctrl_2: SocketAddr = format!("0.0.0.0:{p2}").parse().unwrap();
    let krabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
        (3, format!("127.0.0.1:{p3}").parse().unwrap()),
    ];

    // Slow Krabka pre-vote retries (2s) so they sit well above the JVM's 300ms
    // fetch-timeout — giving the JVM a quiet window between pre-votes to time out
    // the dead leader and promote itself to Prospective (then grant the survivor).
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let mut cfg1 = krabka_controller_config(
        0,
        client_addrs[0],
        krabka_ctrl_1,
        &krabka_voters,
        cluster_id,
        dir1.path(),
    );
    let mut cfg2 = krabka_controller_config(
        1,
        client_addrs[1],
        krabka_ctrl_2,
        &krabka_voters,
        cluster_id,
        dir2.path(),
    );
    cfg1.controller_election_timeout = krabka_units::secs(2);
    cfg2.controller_election_timeout = krabka_units::secs(2);

    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("krabka voter 1 start"),
            s2.await.unwrap().expect("krabka voter 2 start"),
        )
    };

    // JVM voter id 3: release the dead leader fast, self-nominate slowly.
    let props = format!(
        "process.roles=controller\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://0.0.0.0:{p3}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.fetch.timeout.ms=300\n\
         controller.quorum.election.timeout.ms=10000\n\
         log.dirs=/tmp/kraft-controller-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("controller.properties");
    std::fs::write(&proppath, props).unwrap();
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/c.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/c.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTESTED_CONTAINER,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-v",
            &format!("{}:/tmp/c.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM controller");
    assert2::assert!(status.success(), "docker run failed");

    // ── Phase 1: a Krabka node leads and the JVM joins as a follower. ───────
    let deadline = std::time::Instant::now() + Duration::from_secs(50);
    let mut leader0: Option<u64> = None;
    while std::time::Instant::now() < deadline {
        let l1 = c1.controller_leader_id();
        let l2 = c2.controller_leader_id();
        if l1.is_some() && l1 == l2 && matches!(l1, Some(krabka_broker::NodeId(1 | 2))) {
            leader0 = l1.map(|n| n.0);
            break;
        }
        // intentional: cross-checks BOTH in-process voters' leader watches for
        // agreement in {1,2}; no single awaiter expresses the l1 == l2
        // convergence, and awaiting each handle separately would drop this
        // retry and could spuriously trip on a transient election disagreement.
        tokio::time::sleep(Duration::from_millis(500)).await;
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
    let join_deadline = std::time::Instant::now() + Duration::from_secs(70);
    let mut jvm_joined = false;
    let mut last_jvm_log = String::new();
    while std::time::Instant::now() < join_deadline {
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !jvm_joined {
        eprintln!("==== JVM controller logs (tail) — JVM NEVER JOINED ====");
        for line in last_jvm_log
            .lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .iter()
            .rev()
        {
            eprintln!("{line}");
        }
        let _ = std::fs::write("/tmp/jvm_contested.log", &last_jvm_log);
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
    tokio::time::sleep(Duration::from_secs(6)).await;
    eprintln!("phase 1c: JVM has had 6s of steady fetching — killing the leader now");

    // ── Phase 2: kill the Krabka leader; the survivor needs the JVM's grants. ─
    let (killed, survivor, survivor_id) = if leader0 == 1 {
        (c1, c2, 2u64)
    } else {
        (c2, c1, 1u64)
    };
    killed.shutdown().await;
    eprintln!("phase 2: killed Krabka leader {leader0}; survivor is {survivor_id}");

    // ── Phase 3: the surviving Krabka voter must win a new election. ─────────
    // Trace the survivor's quorum state every ~2s so a stuck recovery is legible:
    // does `current_term` climb past epoch0 (the survivor IS promoting in some
    // rounds) or is it truly pinned at the old epoch (no majority reachable)?
    let recover_deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut recovered = false;
    let mut tick = 0u32;
    while std::time::Instant::now() < recover_deadline {
        let qs = survivor.controller_quorum_state_for_test();
        if tick.is_multiple_of(4) {
            eprintln!(
                "[recovery t={}s] survivor {survivor_id} view: leader={:?} term={} (was {epoch0})",
                tick / 2,
                qs.current_leader,
                qs.current_term,
            );
        }
        if qs.current_leader == Some(krabka_broker::NodeId(survivor_id)) && qs.current_term > epoch0
        {
            recovered = true;
            break;
        }
        tick += 1;
        // intentional: waits for the survivor to win at a HIGHER raft term
        // (current_term > epoch0); the controller quorum term is not in the
        // metadata image and has no awaiter/metric — wait_until_controller_leader
        // only observes a non-zero leader, not term advance or leader identity.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let final_qs = survivor.controller_quorum_state_for_test();
    eprintln!(
        "phase 3: survivor view leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader, final_qs.current_term
    );

    let jvm_fatal_fault = capture_contested_jvm_logs();

    docker_rm(CONTESTED_CONTAINER);
    survivor.shutdown().await;

    assert2::assert!(
        recovered,
        "surviving Krabka voter {survivor_id} did not win a new election at a \
         higher epoch after the leader died — the JVM's pre-vote grant was not \
         counted (KIP-996 interop regression). survivor view: leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader,
        final_qs.current_term
    );
    assert2::assert!(
        !jvm_fatal_fault,
        "JVM controller fatal-faulted during the contested election; see /tmp/jvm_contested.log"
    );
}

fn capture_contested_jvm_logs() -> bool {
    // Capture JVM logs for diagnosis regardless of outcome.
    let logs = Command::new("docker")
        .args(["logs", CONTESTED_CONTAINER])
        .output()
        .expect("docker logs");
    let log_text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = std::fs::write("/tmp/jvm_contested.log", &log_text);
    let jvm_fatal_fault = log_text.contains("Encountered fatal fault");

    // Dump the JVM log tail to stderr (pass or fail) — it shows whether the JVM
    // granted/rejected the survivor's preVote/Vote, and whether it tried to
    // become candidate/leader itself.
    eprintln!("==== JVM controller logs (tail) — contested election ====");
    for line in log_text
        .lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .iter()
        .rev()
    {
        eprintln!("{line}");
    }

    jvm_fatal_fault
}
