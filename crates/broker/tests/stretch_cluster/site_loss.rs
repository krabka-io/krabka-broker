//! Losing a whole site by stopping its broker: which survivor leads
//! afterwards, what the in-sync set becomes, and whether `acks=all` keeps
//! committing.
//!
//! This is the site-loss half of the claim the witness role exists for. The
//! unreachable-site half, where every broker stays up, is in
//! `unreachable_site`.

use {
    crate::{
        cluster::{Cluster, cluster_with_topic},
        cluster_lock,
        produce::{client_at, produce_once, produce_until_committed},
        view::{PartitionView, partition_view, wait_for_leader_and_isr, witness_never_leads},
    },
    assert2::{assert, check},
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::{collections::BTreeSet, mem, time::Duration},
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct SiteLoss {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Option<(Cluster, WireUuid)>,
    produce_code: Option<i16>,
}

#[rstest::fixture]
fn site_loss() -> SiteLoss {
    // Every stopped site belongs to one isolated stretch partition.
    SiteLoss::default()
}

impl SiteLoss {
    fn cluster(&self) -> &Cluster { &self.cluster.as_ref().expect("stretch cluster").0 }

    fn topic_id(&self) -> WireUuid { self.cluster.as_ref().expect("stretch cluster").1 }

    async fn shutdown(&mut self) {
        let Self { guard, cluster, .. } = mem::take(self);
        cluster.expect("stretch cluster").0.shutdown().await;
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "a three-site RF3 topic places data brokers 1 and 2 with witness 3 and min.insync.replicas 2"
)]
async fn stretch_topic_for_loss(site_loss: &mut SiteLoss) {
    site_loss.guard = Some(cluster_lock().lock().await);
    site_loss.cluster = Some(cluster_with_topic().await);
}

#[rstest_bdd_macros::when("site broker {node} shuts down")]
async fn stop_stretch_site(site_loss: &mut SiteLoss, node: usize) {
    site_loss
        .cluster
        .as_mut()
        .expect("stretch cluster")
        .0
        .stop(node.checked_sub(1).expect("positive broker id"))
        .await;
}

#[rstest_bdd_macros::then(
    "broker {observer} reports leader {leader}, replicas 1,2,3, ISR {isr} and no adding or removing replicas"
)]
async fn surviving_stretch_partition(
    site_loss: &SiteLoss,
    observer: usize,
    leader: u64,
    isr: String,
) {
    let expected_isr: BTreeSet<u64> = isr
        .split(',')
        .map(|node| node.parse().expect("numeric ISR broker"))
        .collect();
    wait_for_leader_and_isr(
        site_loss
            .cluster()
            .handle(observer.checked_sub(1).expect("positive broker id")),
        "the expected site-loss leader and ISR",
        leader,
        &expected_isr.iter().copied().collect::<Vec<_>>(),
    )
    .await;
    check!(
        partition_view(site_loss.cluster().handle(observer - 1))
            == Some(PartitionView {
                leader,
                replicas: vec![1, 2, 3],
                isr: expected_isr,
                adding_replicas: vec![],
                removing_replicas: vec![]
            }),
        "the witness remains a replica without becoming leader"
    );
}

#[rstest_bdd_macros::when(
    "acks all writes are retried against broker {node} until a committed response"
)]
async fn write_surviving_stretch_site(site_loss: &mut SiteLoss, node: usize) {
    site_loss.produce_code = Some(
        produce_until_committed(
            &site_loss
                .cluster()
                .addr(node.checked_sub(1).expect("positive broker id")),
            site_loss.topic_id(),
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the write returns NONE code 0")]
fn committed_stretch_write(site_loss: &SiteLoss) {
    assert!(
        site_loss.produce_code == Some(0),
        "acks=all must still commit with the surviving two in-sync replicas; code={:?}",
        site_loss.produce_code
    );
}

#[rstest_bdd_macros::then("witness 3 never becomes leader over five seconds")]
async fn no_witness_site_leadership(site_loss: &SiteLoss) {
    witness_never_leads(site_loss.cluster().handle(2), Duration::from_secs(5)).await;
}

#[rstest_bdd_macros::when(
    "a client sends an acks all write directly to witness 3 with a 3000-millisecond timeout"
)]
async fn write_witness_after_site_losses(site_loss: &mut SiteLoss) {
    let witness = client_at(&site_loss.cluster().addr(2)).await;
    site_loss.produce_code = Some(produce_once(&witness, site_loss.topic_id(), 3_000).await);
    witness.close();
}

#[rstest_bdd_macros::then(
    "the write returns NOT_LEADER_OR_FOLLOWER code 6 and the partition view never names witness 3 as leader"
)]
fn refused_witness_after_site_losses(site_loss: &SiteLoss) {
    check!(
        site_loss.produce_code == Some(6),
        "the witness must refuse an acks=all write when both data sites are down; got {:?}",
        site_loss.produce_code
    );
    let view = partition_view(site_loss.cluster().handle(2));
    check!(
        view.as_ref().is_some_and(|view| view.leader != 3),
        "no live broker leads the partition, and the witness is not it: {view:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/stretch_cluster/site_loss.feature",
    name = "preferred site holds replicas zero and the leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_site_holds_replicas_zero_and_the_leader(site_loss: SiteLoss) {
    site_loss.shutdown().await;
    drop(site_loss);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/stretch_cluster/site_loss.feature",
    name = "acks all survives the loss of the non preferred data site"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_survives_the_loss_of_the_non_preferred_data_site(site_loss: SiteLoss) {
    site_loss.shutdown().await;
    drop(site_loss);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/stretch_cluster/site_loss.feature",
    name = "a single site loss keeps acks all committing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_site_loss_keeps_acks_all_committing(
    site_loss: SiteLoss,
    stop: usize,
    observer: usize,
    leader: u64,
    isr: String,
) {
    let _ = (stop, observer, leader, isr);
    site_loss.shutdown().await;
    drop(site_loss);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/stretch_cluster/site_loss.feature",
    name = "both data sites down leaves no leader and the witness refuses writes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_data_sites_down_leaves_no_leader_and_the_witness_refuses_writes(site_loss: SiteLoss) {
    site_loss.shutdown().await;
    drop(site_loss);
}
