//! A bounded Jepsen-style fault schedule over a live diskless WAL quorum.
//!
//! The one case below boots three real brokers, creates an RF=1 diskless
//! topic, pins its sole classic replica onto the broker that also leads the
//! controller, and then takes that broker away. Every acknowledged record must
//! still read back afterwards, from a broker that never held a classic replica
//! of the partition.
//!
//! ## What the schedule proves
//!
//! The faults overlap on purpose, because each one alone has an escape. The
//! object store is blocked before the first produce, so a flush cannot rescue
//! the tail. The acking broker is killed and its canonical log is erased, so a
//! local fsync cannot serve the readback. The controller leader goes with it,
//! so the promotion is a real authority handoff rather than a metadata edit on
//! a live leader. What remains is the WAL quorum on the two survivors, and the
//! readback can only come from there.
//!
//! Two independent readers check the same offsets: a direct Rust partition
//! fetch and the JVM `kafka-console-consumer` in a container. The second one
//! is a byte differential against a client this project does not write.
//!
//! Every fault carries a discriminating witness, so a no-op fails. A blocked
//! PUT is counted on the flusher's own failure metric before and after the
//! crash, the erased log is counted on disk before it is erased, and the two
//! leader handoffs are asserted to name different nodes.
//!
//! ## Why the cluster runs two listeners
//!
//! A diskless quorum in this tree needs an **authenticated** inter-broker
//! listener. A WAL follower's Fetch is authorized against the caller's
//! principal: the leader resolves `broker-<id>` to a node id and serves the
//! shard only to a voter that is who it claims to be. That convention applies
//! only to connections on the inter-broker listener, so an anonymous plaintext
//! cluster never forms a quorum at all.
//!
//! The JVM console consumer runs in a container and has to reach the brokers
//! from there, which means the client-facing endpoint must advertise
//! `host.docker.internal`. Putting SASL on that same endpoint would make the
//! container hold credentials for a test whose subject is durability, not
//! authentication.
//!
//! So each broker binds two data-plane listeners: a `PLAINTEXT` one on all
//! interfaces that advertises `host.docker.internal` and carries every client
//! in the suite, and a `SASL_PLAINTEXT` one on loopback that
//! `inter_broker_listener_name` selects and that carries the raft, replication
//! and WAL follower traffic. `BrokerConfig.listeners` is a list, so this costs
//! one more `ListenerSpec` per broker.
//!
//! ## Layout
//!
//! The binary root carries the module tree, the constants every part shares
//! and the one case. [`cluster`] boots the three brokers and drives their
//! metadata, [`history`] produces the ledger and checks it for
//! linearizability, [`faults`] injects each fault and holds the witness, and
//! [`readback`] reads the acknowledged offsets back with both clients.
#![cfg(test)]

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `diskless_jepsen/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "diskless_jepsen/cluster.rs"]
mod cluster;
#[path = "diskless_jepsen/faults.rs"]
mod faults;
#[path = "diskless_jepsen/history.rs"]
mod history;
#[path = "diskless_jepsen/readback.rs"]
mod readback;

use {assert2::assert, tempfile::TempDir};

/// The one diskless topic this suite creates. The name is
/// `[A-Za-z0-9_-]`-only, so it reaches the filesystem unchanged and
/// [`faults`] can build the WAL and log paths by hand.
const TOPIC: &str = "diskless-jepsen";

/// Concurrent producers on the accepting broker. Two is the smallest number
/// that gives the linearizability checker a real choice of orders.
const APPENDERS: u64 = 2;

/// Records each appender sends. The ledger stays small enough that one Fetch
/// and one `--max-messages` run read the whole of it.
const RECORDS_PER_APPENDER: u64 = 4;

/// The diskless WAL quorum this suite runs: three voters, so the loss of one
/// still leaves a strict majority.
const VOTERS: usize = 3;

/// The one password every SASL PLAIN principal in the suite authenticates
/// with. Only the inter-broker listener asks for it; the clients use the
/// plaintext listener.
const PASSWORD: &str = "diskless-jepsen";

use {
    crate::{
        cluster::TestNode,
        history::{AckedRecord, LedgerEntry},
    },
    krabka_broker::NodeId,
    std::{iter, mem, path::PathBuf, process::Output},
};
#[derive(Default)]
struct DisklessFaultSchedule {
    cluster: Vec<TestNode>,
    blocker: PathBuf,
    victim: Option<usize>,
    survivors: Vec<usize>,
    old_controller: Option<NodeId>,
    new_controller: Option<NodeId>,
    promoted: Option<usize>,
    ledger: Vec<AckedRecord>,
    rust_readback: Option<Vec<LedgerEntry>>,
    jvm_output: Option<Output>,
    put_failures_before: u64,
    put_failures_after: u64,
    observed_failures: Option<u64>,
    object_directory: Option<TempDir>,
}
#[rstest::fixture]
fn diskless_fault_schedule() -> DisklessFaultSchedule {
    // Each fault schedule owns its physical PUT blocker, broker disks and real history.
    DisklessFaultSchedule::default()
}
#[rstest_bdd_macros::given(
    "a three-voter RF1 diskless topic whose sole replica is the controller leader and whose object PUT namespace is blocked"
)]
async fn jepsen_blocked_quorum(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let object_dir = TempDir::new().expect("shared object dir");
    let put_blocker = faults::block_object_puts(object_dir.path());

    let cluster = cluster::start_jepsen_cluster(object_dir.path()).await;
    cluster::await_brokers_registered(&cluster).await;
    cluster::create_diskless_topic(&cluster[0].client_bootstrap()).await;
    cluster::await_topic_placement(&cluster).await;

    // Make one exact broker both the accepting partition leader and the
    // controller leader. Killing it therefore exercises the accepting-broker
    // fault and a sequencer/controller-authority handoff in the same bounded
    // schedule, while two of three voters remain live.
    let old_controller = cluster::converged_controller_leader(&cluster).await;
    let victim = cluster::index_of(&cluster, old_controller);
    let all: Vec<usize> = (0..cluster.len()).collect();
    let survivors: Vec<usize> = all.iter().copied().filter(|i| *i != victim).collect();
    cluster::force_partition_owner(&cluster, &all, 0, old_controller).await;
    cluster::await_wal_runtime(&cluster[victim], old_controller).await;
    cluster::assert_sole_classic_owner(&cluster, victim, &survivors, old_controller).await;

    diskless_fault_schedule.put_failures_before = cluster[victim]
        .handle()
        .diskless_put_failure_count_for_test();
    diskless_fault_schedule.cluster = cluster;
    diskless_fault_schedule.blocker = put_blocker;
    diskless_fault_schedule.victim = Some(victim);
    diskless_fault_schedule.survivors = survivors;
    diskless_fault_schedule.old_controller = Some(old_controller);
    diskless_fault_schedule.object_directory = Some(object_dir);
}
#[rstest_bdd_macros::when(
    "two concurrent idempotent appenders each produce four records with acks-all"
)]
async fn jepsen_concurrent_history(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let victim = diskless_fault_schedule.victim.expect("accepting replica");
    diskless_fault_schedule.ledger =
        history::produce_concurrently(&diskless_fault_schedule.cluster[victim].client_bootstrap())
            .await;
}
#[rstest_bdd_macros::then(
    "the acknowledged {count} record ledger is gap-free, unique and linearizable in real-time order"
)]
fn jepsen_linearizable_ledger(diskless_fault_schedule: &DisklessFaultSchedule, count: usize) {
    assert!(diskless_fault_schedule.ledger.len() == count);
    history::assert_acked_ledger(&diskless_fault_schedule.ledger);
    history::assert_linearizable_history(&diskless_fault_schedule.ledger);
}
#[rstest_bdd_macros::when(
    "the blocked flusher fails a PUT and both nonreplica voters checkpoint the acknowledged prefix"
)]
async fn jepsen_fault_ready(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let cluster = &diskless_fault_schedule.cluster;
    let victim = diskless_fault_schedule.victim.expect("accepting replica");
    let ledger = &diskless_fault_schedule.ledger;
    let object_dir = diskless_fault_schedule
        .object_directory
        .as_ref()
        .expect("object directory");
    let put_blocker = &diskless_fault_schedule.blocker;
    let survivors = &diskless_fault_schedule.survivors;
    let put_failures_before = diskless_fault_schedule.put_failures_before;
    let durable_end = history::durable_end(ledger);
    faults::await_put_failure(&cluster[victim], put_failures_before).await;
    faults::assert_puts_still_blocked(object_dir.path(), put_blocker);
    cluster[victim]
        .handle()
        .wait_until_local_log_end_offset(TOPIC, 0, durable_end)
        .await;
    for &index in survivors {
        faults::await_follower_checkpoint(&cluster[index], durable_end).await;
    }
}
#[rstest_bdd_macros::when(
    "the accepting controller is crashed and its canonical log erased before a surviving controller and partition leader take over"
)]
async fn jepsen_authority_handoff(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let cluster = &mut diskless_fault_schedule.cluster;
    let victim = diskless_fault_schedule.victim.expect("accepting replica");
    let survivors = &diskless_fault_schedule.survivors;
    let old_controller = diskless_fault_schedule
        .old_controller
        .expect("old controller");
    let durable_end = history::durable_end(&diskless_fault_schedule.ledger);
    faults::crash_and_erase(cluster, victim).await;
    let new_controller = faults::converged_new_controller(cluster, survivors, old_controller).await;
    let promoted = cluster::index_of(cluster, new_controller);
    cluster::force_partition_owner(cluster, survivors, promoted, new_controller).await;
    cluster::await_promoted_leader(&cluster[promoted], new_controller, durable_end).await;
    diskless_fault_schedule.new_controller = Some(new_controller);
    diskless_fault_schedule.promoted = Some(promoted);
}
#[rstest_bdd_macros::then(
    "both authority leaders differ from the erased WAL node and object PUTs are still blocked"
)]
fn jepsen_changed_authority(diskless_fault_schedule: &DisklessFaultSchedule) {
    let old = diskless_fault_schedule
        .old_controller
        .expect("erased controller/partition leader");
    let new = diskless_fault_schedule
        .new_controller
        .expect("new controller/partition leader");
    assert!(old != new);
    assert!(
        diskless_fault_schedule.cluster[diskless_fault_schedule.victim.expect("victim")]
            .config
            .node_id
            == old
    );
    assert!(diskless_fault_schedule.blocker.is_file());
}
#[rstest_bdd_macros::when(
    "the Rust partition reader fetches the acknowledged ledger from a survivor while object PUTs remain blocked"
)]
async fn jepsen_rust_readback(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let survivor = diskless_fault_schedule.survivors[0];
    diskless_fault_schedule.rust_readback = Some(
        readback::consume_ledger(
            &diskless_fault_schedule.cluster[survivor].client_bootstrap(),
            diskless_fault_schedule.ledger.len(),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the Rust offsets and payload bytes exactly equal the acknowledged ledger"
)]
fn jepsen_expected_rust_ledger(diskless_fault_schedule: &DisklessFaultSchedule) {
    let expected = history::ledger_values(&diskless_fault_schedule.ledger);
    assert!(
        diskless_fault_schedule
            .rust_readback
            .as_ref()
            .expect("Rust fetch executed")
            == &expected
    );
}
#[rstest_bdd_macros::when(
    "the replacement flusher fails another PUT before the object namespace is unblocked and a WAL object is written"
)]
async fn jepsen_retry_object_put(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let promoted = diskless_fault_schedule.promoted.expect("promoted node");
    let node = &diskless_fault_schedule.cluster[promoted];
    let before = node.handle().diskless_put_failure_count_for_test();
    faults::report_flush_state(node, before).await;
    let failures = faults::await_put_failure(node, before).await;
    let dir = diskless_fault_schedule
        .object_directory
        .as_ref()
        .expect("object directory");
    faults::unblock_object_puts(dir.path(), &diskless_fault_schedule.blocker);
    faults::await_wal_object(dir.path(), node.config.broker_id).await;
    diskless_fault_schedule.put_failures_after = before;
    diskless_fault_schedule.observed_failures = Some(failures);
}
#[rstest_bdd_macros::then(
    "the replacement flusher's failure counter advanced and the object namespace is a directory"
)]
fn jepsen_object_retry(diskless_fault_schedule: &DisklessFaultSchedule) {
    assert!(
        diskless_fault_schedule
            .observed_failures
            .expect("failed replacement PUT")
            > diskless_fault_schedule.put_failures_after
    );
    assert!(diskless_fault_schedule.blocker.is_dir());
}
#[rstest_bdd_macros::when(
    "the official JVM console consumer drains the acknowledged ledger through a survivor"
)]
async fn jepsen_jvm_readback(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    let survivor = diskless_fault_schedule.survivors[0];
    diskless_fault_schedule.jvm_output = Some(
        readback::jvm_consume_exact(
            &diskless_fault_schedule.cluster[survivor].docker_bootstrap(),
            diskless_fault_schedule.ledger.len(),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the JVM consumer succeeds with exactly the acknowledged payload bytes in offset order, one per line"
)]
fn jepsen_expected_jvm_ledger(diskless_fault_schedule: &DisklessFaultSchedule) {
    let output = diskless_fault_schedule
        .jvm_output
        .as_ref()
        .expect("JVM reader executed");
    assert!(output.status.success(), "JVM consumer failed: {:?}", output);
    let expected = history::ledger_values(&diskless_fault_schedule.ledger)
        .iter()
        .flat_map(|entry| entry.value.iter().copied().chain(iter::once(b'\n')))
        .collect::<Vec<_>>();
    assert!(output.stdout == expected);
}
#[rstest_bdd_macros::then("the diskless fault-schedule cluster is shut down")]
async fn finish_diskless_schedule(diskless_fault_schedule: &mut DisklessFaultSchedule) {
    cluster::shutdown(mem::take(&mut diskless_fault_schedule.cluster)).await;
}
#[ignore = "requires Docker; Linux-bound (host.docker.internal bridge)"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/diskless_jepsen.feature",
    name = "three broker fault schedule preserves the acked ledger"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_broker_fault_schedule_preserves_the_acked_ledger(
    diskless_fault_schedule: DisklessFaultSchedule,
) {
    let _ = diskless_fault_schedule;
}
