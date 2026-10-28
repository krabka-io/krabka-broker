//! Only leader storage is claimed: container-advertised names need not route follower fetches under WSL.
use {
    crate::authenticated_peers::{AuthenticatedPeers, peers, shutdown},
    rstest_bdd_macros::scenario,
};
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_peer_replication.feature",
    name = "Stock clients write through PLAIN authenticated peers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_inter_broker_replication_authed(peers: AuthenticatedPeers) { shutdown(peers).await; }
