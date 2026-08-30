//! The join loop proper: retry `AddRaftVoter` against the bootstrap servers
//! until this node's own identity appears in the committed voter set.
//!
//! The loop also repairs a stale registration: if the committed voter set
//! already names this node under a different directory identity, it sends
//! `RemoveRaftVoter` for the stale entry first, then re-joins on a later
//! iteration.

use krabka_protocol::owned::remove_raft_voter_request::RemoveRaftVoterRequest;
use krabka_units::convert::TimeExt as _;

use super::{
    AutoJoinParams,
    outcome::{JoinOutcome, log_join_outcome},
    request::{build_add_raft_voter_request, controller_listener, select_bootstrap_server},
    rpc::{send_add_raft_voter, send_remove_raft_voter},
};

/// Drive the auto-join loop. Returns immediately (without touching the
/// network) when `auto_join` is disabled. Otherwise loops until this broker
/// appears in the committed voter set, rotating across `bootstrap_servers`.
/// Intended to be spawned as a detached background task during `Broker::start`.
pub(crate) async fn run(params: AutoJoinParams) {
    if !params.auto_join {
        return;
    }

    let self_id = params.node_id;
    let bootstrap_servers = params.bootstrap_servers;
    if bootstrap_servers.is_empty() {
        tracing::warn!(
            node_id = self_id.0,
            "auto_join enabled but bootstrap_servers is empty; cannot discover a leader"
        );
        return;
    }

    // Self's voter identity, advertising the REAL bound controller endpoint
    // (resolved port, not the possibly-zero configured port) so the leader's
    // add_learner can dial us back.
    let bound = params.controller.controller_bound_addr();
    let Ok(voter_id) = i32::try_from(self_id.0) else {
        tracing::error!(node_id = self_id.0, "node_id exceeds i32; cannot auto-join");
        return;
    };
    let directory_id = krabka_protocol::primitives::uuid::Uuid(*params.directory_id.as_bytes());
    let listener = controller_listener(bound);

    let protocol = params.listener_protocol;
    let server_name = params.inter_broker_server_name;
    let retry_backoff = params.retry_backoff;
    let Ok(voter_request_timeout_ms) = i32::try_from(params.voter_request_timeout.millis_i64())
    else {
        tracing::error!(
            timeout = ?params.voter_request_timeout,
            "auto-join voter request timeout exceeds Kafka wire limit"
        );
        return;
    };
    let client = params.inter_broker_client;
    let controller = params.controller;
    let cluster_id = params.cluster_id;

    let mut next_server = 0usize;
    loop {
        // Terminate as soon as the committed voter set includes us.
        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id == params.directory_id
        {
            tracing::info!(node_id = self_id.0, "auto-join complete; node is a voter");
            return;
        }

        let target = select_bootstrap_server(&bootstrap_servers, next_server);
        next_server = next_server.wrapping_add(1);

        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id != params.directory_id
        {
            let req = RemoveRaftVoterRequest {
                cluster_id: cluster_id.map(|id| id.to_string()),
                voter_id,
                voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                    *existing.directory_id.as_bytes(),
                ),
                ..Default::default()
            };
            if let Err(error) =
                send_remove_raft_voter(&client, protocol, &server_name, target, &req).await
            {
                tracing::debug!(node_id = self_id.0, server = %target, %error, "auto-join: stale voter removal failed");
            }
            tokio::time::sleep(retry_backoff.to_std()).await;
            continue;
        }

        let req = build_add_raft_voter_request(
            cluster_id,
            voter_id,
            directory_id,
            listener.clone(),
            voter_request_timeout_ms,
        );

        match send_add_raft_voter(&client, protocol, &server_name, target, &req).await {
            Ok(resp) => {
                let _: JoinOutcome = log_join_outcome(self_id, target, &resp);
            }
            Err(e) => {
                tracing::debug!(
                    node_id = self_id.0,
                    server = %target,
                    error = %e,
                    "auto-join: dial/RPC failed; trying next bootstrap server"
                );
            }
        }

        tokio::time::sleep(retry_backoff.to_std()).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use krabka_metadata::{
        KRaftVersionRange, MetadataImage, MetadataRecord, Voter, VoterEndpoint, VoterSet,
        VotersRecord,
    };
    use krabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
        SnapshotRange, UpdateVoter,
    };
    use krabka_units::{millis, secs};
    use tokio::sync::watch;

    use super::*;

    struct MockSource {
        image: Arc<MetadataImage>,
        current_image_calls: AtomicUsize,
        controller_bound_addr_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.current_image_calls.fetch_add(1, Ordering::Relaxed);
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            let (_tx, rx) = watch::channel(None);
            rx
        }

        fn quorum_state(&self) -> QuorumState {
            panic!("not used by auto_join tests")
        }

        async fn submit_change(
            &self,
            _records: Vec<krabka_metadata::MetadataRecord>,
        ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            self.controller_bound_addr_calls
                .fetch_add(1, Ordering::Relaxed);
            "127.0.0.1:19093".parse().expect("bound controller addr")
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            panic!("not used by auto_join tests")
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn cancel(&self) {}
    }

    fn image_with_voter(node_id: NodeId) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::from_voters([Voter {
                id: node_id,
                directory_id: uuid::Uuid::from_u128(node_id.0.into()),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19093,
                }],
                kraft_version: KRaftVersionRange::default(),
            }]),
        }));
        image
    }

    /// `run` returns immediately when `auto_join` is disabled — no panic, no
    /// network dial. Build params with a real controller + inter-broker client
    /// but `auto_join = false`, and a deliberately bogus bootstrap server. If
    /// `run` honoured the flag it never dials; if it regressed and dialed, the
    /// loop would spin against the unreachable address and the timeout would
    /// fire (failing the test).
    #[tokio::test]
    async fn run_returns_immediately_when_auto_join_disabled() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config = crate::BrokerConfig::for_tests(tempdir.path().to_path_buf());
        let handle = crate::Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        let params = AutoJoinParams {
            auto_join: false,
            retry_backoff: millis(7),
            voter_request_timeout: secs(30),
            node_id: krabka_raft::NodeId(999),
            directory_id: uuid::Uuid::from_u128(1),
            cluster_id: None,
            // Unroutable: would hang the loop if `run` ignored auto_join=false.
            bootstrap_servers: vec!["127.0.0.1:1".to_string()],
            listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "broker.internal".to_string(),
            controller: broker.controller_for_test(),
            inter_broker_client: broker.inter_broker_client_for_test(),
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("run() returned immediately for auto_join=false");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn run_with_auto_join_true_checks_current_voter_set_before_returning() {
        let source = Arc::new(MockSource {
            image: Arc::new(image_with_voter(NodeId(7))),
            current_image_calls: AtomicUsize::new(0),
            controller_bound_addr_calls: AtomicUsize::new(0),
        });
        let params = AutoJoinParams {
            auto_join: true,
            retry_backoff: millis(7),
            voter_request_timeout: secs(30),
            node_id: krabka_raft::NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            cluster_id: None,
            bootstrap_servers: vec!["127.0.0.1:1".to_string()],
            listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "broker.internal".to_string(),
            controller: source.clone(),
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("already-voter auto join returns without dialing");

        assert2::assert!((source.controller_bound_addr_calls.load(Ordering::Relaxed)) == (1));
        assert2::assert!((source.current_image_calls.load(Ordering::Relaxed)) == (1));
    }
}
