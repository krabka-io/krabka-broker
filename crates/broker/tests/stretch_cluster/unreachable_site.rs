//! A site that is unreachable rather than stopped: every broker stays up and
//! the network in front of one site is taken away instead.
//!
//! This is the half of the partition claim the relay harness can produce
//! honestly — what an isolated *leader* must stop doing — and it is written
//! against the one-way cut the crate documentation describes.

use {
    crate::{
        NODE_A, NODE_C, WITNESS, cluster_lock,
        linked::{LinkedCluster, linked_cluster_with_topic},
        produce::{client_at, produce_once, produce_until_committed},
        view::{wait_for_leader_and_isr, witness_never_leads},
    },
    assert2::check,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::{mem, time::Duration},
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct UnreachableLeader {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Option<(LinkedCluster, WireUuid)>,
    leader_client: Option<Client>,
    code: Option<i16>,
}

#[rstest::fixture]
fn unreachable_leader() -> UnreachableLeader {
    // The leader remains live through the incoming one-way cut and heal.
    UnreachableLeader::default()
}

impl UnreachableLeader {
    fn cluster(&self) -> &LinkedCluster {
        &self.cluster.as_ref().expect("linked stretch cluster").0
    }

    fn topic_id(&self) -> WireUuid { self.cluster.as_ref().expect("linked stretch cluster").1 }

    async fn shutdown(&mut self) {
        let Self {
            guard,
            cluster,
            leader_client,
            ..
        } = mem::take(self);
        if let Some(client) = leader_client {
            client.close();
        }
        cluster.expect("linked stretch cluster").0.shutdown().await;
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "a live three-site RF3 partition has data leader 1 and an incoming relay in front of each site's listeners"
)]
async fn reachable_stretch_leader(unreachable_leader: &mut UnreachableLeader) {
    unreachable_leader.guard = Some(cluster_lock().lock().await);
    unreachable_leader.cluster = Some(linked_cluster_with_topic().await);
}

#[rstest_bdd_macros::when(
    "acks all writes are retried against the live leader until a committed response"
)]
async fn write_reachable_stretch_leader(unreachable_leader: &mut UnreachableLeader) {
    unreachable_leader.code = Some(
        produce_until_committed(
            &unreachable_leader.cluster().addr(NODE_A),
            unreachable_leader.topic_id(),
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the live leader returns NONE code 0")]
fn committed_reachable_stretch_leader(unreachable_leader: &UnreachableLeader) {
    check!(
        unreachable_leader.code == Some(0),
        "acks=all commits while every site is reachable"
    );
}

#[rstest_bdd_macros::when("site 1's incoming relay is cut while every broker remains running")]
fn cut_live_stretch_leader(unreachable_leader: &UnreachableLeader) {
    unreachable_leader.cluster().cut(NODE_A);
}

#[rstest_bdd_macros::when(
    "a direct client sends the live leader one acks all write with a 3000-millisecond timeout"
)]
async fn write_unreachable_stretch_leader(unreachable_leader: &mut UnreachableLeader) {
    if unreachable_leader.leader_client.is_none() {
        unreachable_leader.leader_client =
            Some(client_at(&unreachable_leader.cluster().addr(NODE_A)).await);
    }
    unreachable_leader.code = Some(
        produce_once(
            unreachable_leader
                .leader_client
                .as_ref()
                .expect("direct leader client"),
            unreachable_leader.topic_id(),
            3_000,
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the stranded write is not acknowledged with NONE")]
fn unacknowledged_stranded_stretch_write(unreachable_leader: &UnreachableLeader) {
    check!(
        unreachable_leader.code.expect("stranded write answered") != 0,
        "a write only the unreachable leader holds must not be acknowledged"
    );
}

#[rstest_bdd_macros::then("the unreachable leader remains node 1 with ISR containing only 1")]
async fn isolated_stretch_isr(unreachable_leader: &UnreachableLeader) {
    wait_for_leader_and_isr(
        unreachable_leader.cluster().handle(NODE_A),
        "the ISR shrinks to the unreachable leader alone",
        1,
        &[1],
    )
    .await;
}

#[rstest_bdd_macros::then("the live leader refuses the write with NOT_ENOUGH_REPLICAS code 19")]
fn refused_unreachable_stretch_write(unreachable_leader: &UnreachableLeader) {
    check!(
        unreachable_leader.code == Some(19),
        "a leader its replicas cannot reach must refuse an acks=all write rather than acknowledge one nothing else holds; got {:?}",
        unreachable_leader.code
    );
}

#[rstest_bdd_macros::then("witness 3 never becomes leader over three seconds")]
async fn unreachable_witness_never_leads(unreachable_leader: &UnreachableLeader) {
    witness_never_leads(
        unreachable_leader.cluster().handle(NODE_C),
        Duration::from_secs(3),
    )
    .await;
}

#[rstest_bdd_macros::when("site 1's incoming relay heals")]
fn heal_live_stretch_leader(unreachable_leader: &UnreachableLeader) {
    unreachable_leader.cluster().heal(NODE_A);
}

#[rstest_bdd_macros::then("leader 1's ISR returns to data replicas 1,2 and witness 3")]
async fn healed_stretch_isr(unreachable_leader: &UnreachableLeader) {
    wait_for_leader_and_isr(
        unreachable_leader.cluster().handle(NODE_A),
        "the ISR returns to three replicas after the heal",
        1,
        &[1, 2, WITNESS],
    )
    .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/stretch_cluster/unreachable_site.feature",
    name = "an unreachable leader stops acknowledging acks all and recovers on heal"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_leader_stops_acknowledging_acks_all_and_recovers_on_heal(
    unreachable_leader: UnreachableLeader,
) {
    unreachable_leader.shutdown().await;
    drop(unreachable_leader);
}
