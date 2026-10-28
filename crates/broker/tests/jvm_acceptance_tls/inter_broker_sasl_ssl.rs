//! Both local logs prove RF2 follower replication over the encrypted authenticated stack.
use {
    crate::authenticated_peers::{AuthenticatedPeers, peers, shutdown},
    rstest_bdd_macros::scenario,
};
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_peer_replication.feature",
    name = "Stock clients replicate records through SASL SSL peers and controllers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_inter_broker_sasl_ssl_raft_replication(peers: AuthenticatedPeers) {
    shutdown(peers).await;
}
