//! The metadata-sharing proof itself: kill the partition leader and read every
//! record back from the survivor.
//!
//! The suite has exactly one test, and it is long because the scenario is: boot
//! three brokers, produce until segments tier, watch the follower evict its own
//! copy of what the leader tiered, wait for the follower's RLMM consumer to
//! catch up, shut the leader down, wait for the surviving quorum to elect the
//! follower, then consume from offset 0. The steps that stand on their own live
//! in the sibling modules; what remains here is the ordering between them and
//! the discriminating assertion, which must keep requiring every record back.

use {
    crate::{
        RECORDS, TOPIC,
        multi_client::fetch_all_records,
        multi_cluster::{
            await_all_brokers_registered, await_all_rlmm_active, await_pair_leader,
            start_three_tiered_brokers,
        },
        multi_workload::{
            await_follower_local_eviction, create_tiered_topic, produce_and_await_remote_segments,
        },
        support::client::connect_owned,
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, NodeId},
    krabka_client_core::Client,
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
};

#[derive(Default)]
struct TieredSurvivor {
    brokers: [Option<BrokerHandle>; 3],
    log_dirs: Vec<TempDir>,
    remote_dir: Option<TempDir>,
    admin: Option<Client>,
    leader: Option<u64>,
    follower: Option<u64>,
    follower_addr: String,
    served: Option<usize>,
}

#[rstest::fixture]
fn tiered_survivor() -> TieredSurvivor {
    // The follower's local eviction must be observed before its election.
    TieredSurvivor::default()
}

impl TieredSurvivor {
    fn broker(&self, index: usize) -> &BrokerHandle {
        self.brokers[index].as_ref().expect("live tiered broker")
    }

    fn admin(&self) -> &Client { self.admin.as_ref().expect("tiered admin") }

    async fn shutdown(self) {
        let Self {
            brokers,
            admin,
            log_dirs,
            remote_dir,
            ..
        } = self;
        drop(admin);
        for broker in brokers.into_iter().flatten() {
            broker.shutdown().await;
        }
        drop((log_dirs, remote_dir));
    }
}

#[rstest_bdd_macros::given(
    "three registered brokers share a local archive and RF3 topic metadata, while an RF2 tiered data partition spans brokers 1 and 2"
)]
async fn survivor_metadata_topology(tiered_survivor: &mut TieredSurvivor) {
    let (b1, b2, b3, log_dirs, remote_dir) = start_three_tiered_brokers().await;
    tiered_survivor.brokers = [Some(b1), Some(b2), Some(b3)];
    tiered_survivor.log_dirs = log_dirs;
    tiered_survivor.remote_dir = Some(remote_dir);
    await_all_brokers_registered(
        tiered_survivor.broker(0),
        tiered_survivor.broker(1),
        tiered_survivor.broker(2),
    )
    .await;
    await_all_rlmm_active(
        tiered_survivor.broker(0),
        tiered_survivor.broker(1),
        tiered_survivor.broker(2),
    )
    .await;
    tiered_survivor.admin = Some(
        connect_owned(
            format!(
                "127.0.0.1:{}",
                tiered_survivor.broker(0).listen_addr().port()
            ),
            "tiered-multi-admin",
            "admin client",
        )
        .await,
    );
    create_tiered_topic(
        tiered_survivor.admin(),
        tiered_survivor.broker(0),
        tiered_survivor.broker(1),
    )
    .await;
    let (b1_id, b2_id) = await_pair_leader(
        tiered_survivor.broker(0),
        tiered_survivor.broker(1),
        TOPIC,
        0,
    )
    .await;
    let (leader, follower, follower_index) = if tiered_survivor
        .broker(0)
        .partition_leader_for_test(TOPIC, 0)
        == Some(b1_id)
    {
        (b1_id, b2_id, 1)
    } else {
        (b2_id, b1_id, 0)
    };
    tiered_survivor.leader = Some(leader);
    tiered_survivor.follower = Some(follower);
    tiered_survivor.follower_addr = format!(
        "127.0.0.1:{}",
        tiered_survivor.broker(follower_index).listen_addr().port()
    );
}

#[rstest_bdd_macros::when(
    "160 acknowledged records are produced and their sealed segments are copied to the shared archive"
)]
async fn archive_survivor_records(tiered_survivor: &TieredSurvivor) {
    produce_and_await_remote_segments(
        tiered_survivor.admin(),
        tiered_survivor
            .remote_dir
            .as_ref()
            .expect("shared archive")
            .path(),
    )
    .await;
}

#[rstest_bdd_macros::then(
    "the follower evicts all sealed local segments while the original leader is still running"
)]
async fn evicted_survivor_records(tiered_survivor: &TieredSurvivor) {
    let follower = usize::try_from(tiered_survivor.follower.expect("follower node"))
        .expect("small node id")
        - 1;
    await_follower_local_eviction(tiered_survivor.log_dirs[follower].path()).await;
}

#[rstest_bdd_macros::when(
    "eight seconds allow the follower's metadata consumer to catch up and the original leader shuts down"
)]
async fn stop_survivor_source(tiered_survivor: &mut TieredSurvivor) {
    time::sleep(Duration::from_secs(8)).await;
    drop(tiered_survivor.admin.take());
    let leader =
        usize::try_from(tiered_survivor.leader.expect("leader node")).expect("small node id") - 1;
    tiered_survivor.brokers[leader]
        .take()
        .expect("old leader")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::when(
    "the surviving quorum elects the follower and its metadata manager settles for three seconds"
)]
async fn elect_survivor(tiered_survivor: &TieredSurvivor) {
    let follower = usize::try_from(tiered_survivor.follower.expect("follower node"))
        .expect("small node id")
        - 1;
    tiered_survivor
        .broker(follower)
        .wait_until_partition_leader_changed(
            TOPIC,
            0,
            NodeId(tiered_survivor.leader.expect("old leader")),
        )
        .await;
    time::sleep(Duration::from_secs(3)).await;
}

#[rstest_bdd_macros::when(
    "a consumer fetches all 160 records from the survivor starting at offset zero"
)]
async fn fetch_survivor_records(tiered_survivor: &mut TieredSurvivor) {
    tiered_survivor.served = Some(
        fetch_all_records(
            &tiered_survivor.follower_addr,
            TOPIC,
            0,
            0,
            RECORDS,
            Instant::now() + Duration::from_mins(1),
        )
        .await,
    );
}

#[rstest_bdd_macros::then(
    "the survivor serves at least 160 records using metadata learned before its election"
)]
fn complete_survivor_records(tiered_survivor: &TieredSurvivor) {
    let served = tiered_survivor.served.expect("survivor fetched");
    assert!(
        served >= RECORDS,
        "expected >= {RECORDS} records served by the surviving broker via the remote tier; got {served}. The survivor should have learned segment locations from __remote_log_metadata (rf=3) without having run the copy task itself."
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_multi_broker/multi_failover.feature",
    name = "tiered storage metadata sharing via survivor"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiered_storage_metadata_sharing_via_survivor(tiered_survivor: TieredSurvivor) {
    tiered_survivor.shutdown().await;
}
