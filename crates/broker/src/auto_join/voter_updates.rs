//! The `UpdateRaftVoter` advertisement loop.
//!
//! This is the second, independent background task of the module: it tells the
//! current leader where this controller actually listens, at startup and again
//! after every leader change. It is separate from the join loop because it
//! keeps running for the life of the broker, whereas the join loop stops as
//! soon as the node is a voter.

use krabka_protocol::owned::update_raft_voter_request::{
    KRaftVersionFeature, Listener as UpdateListener, UpdateRaftVoterRequest,
};
use krabka_units::convert::TimeExt as _;

use super::{
    AutoJoinParams,
    request::{controller_listener, select_bootstrap_server},
    rpc::send_update_voter,
};
use crate::codes;

/// Advertise this controller at startup and after each leader change. The
/// leader accepts this at both `kraft.version` levels; level zero keeps the
/// data in memory for upgrade preflight, while level one persists it.
pub(crate) async fn run_voter_updates(params: AutoJoinParams) {
    let Ok(voter_id) = i32::try_from(params.node_id.0) else {
        tracing::error!(
            node_id = params.node_id.0,
            "node_id exceeds i32; cannot update voter"
        );
        return;
    };
    let listener = controller_listener(params.controller.controller_bound_addr());
    let mut last_updated = None;
    let mut next_server = 0usize;
    loop {
        let quorum = params.controller.quorum_state();
        let leader = quorum.current_leader;
        let epoch = i32::try_from(quorum.current_term).unwrap_or(i32::MAX);
        if leader.is_some() && last_updated != Some((leader, epoch)) {
            // If our advertised listener and directory ID already match the committed
            // voter record, skip sending an update RPC.
            if let Some(my_voter) = quorum.voter_nodes.get(&params.node_id) {
                let matches_dir = my_voter.directory_id == params.directory_id;
                let matches_listener = my_voter
                    .endpoints
                    .iter()
                    .any(|ep| ep.host == listener.host && ep.port == listener.port);
                if matches_dir && matches_listener {
                    last_updated = Some((leader, epoch));
                    tokio::time::sleep(params.retry_backoff.to_std()).await;
                    continue;
                }
            }

            // Resolve target controller: try the known leader's endpoint first,
            // falling back to bootstrap_servers only when unmapped.
            let target_str = if let Some(leader_id) = leader
                && let Some(leader_node) = quorum.voter_nodes.get(&leader_id)
                && let Some(ep) = leader_node
                    .endpoints
                    .iter()
                    .find(|e| e.name.eq_ignore_ascii_case("CONTROLLER"))
                    .or_else(|| leader_node.endpoints.first())
            {
                Some(format!("{}:{}", ep.host, ep.port))
            } else if !params.bootstrap_servers.is_empty() {
                let s = select_bootstrap_server(&params.bootstrap_servers, next_server);
                next_server = next_server.wrapping_add(1);
                Some(s.to_string())
            } else {
                None
            };

            let Some(target) = target_str else {
                tracing::warn!(
                    node_id = params.node_id.0,
                    ?leader,
                    "no target controller endpoint or bootstrap server available for UpdateVoter"
                );
                tokio::time::sleep(params.retry_backoff.to_std()).await;
                continue;
            };
            let request = UpdateRaftVoterRequest {
                cluster_id: params.cluster_id.map(|id| id.to_string()),
                current_leader_epoch: epoch,
                voter_id,
                voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                    *params.directory_id.as_bytes(),
                ),
                listeners: vec![UpdateListener {
                    name: listener.name.clone(),
                    host: listener.host.clone(),
                    port: listener.port,
                    ..Default::default()
                }],
                k_raft_version_feature: KRaftVersionFeature {
                    min_supported_version: 0,
                    max_supported_version: 1,
                    ..Default::default()
                },
                ..Default::default()
            };
            match send_update_voter(
                &params.inter_broker_client,
                params.listener_protocol,
                &params.inter_broker_server_name,
                &target,
                &request,
            )
            .await
            {
                Ok(response) if response.error_code == codes::NONE => {
                    last_updated = Some((leader, epoch));
                }
                Ok(response) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    error_code = response.error_code,
                    "UpdateVoter was not acknowledged; retrying"
                ),
                Err(error) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    %error,
                    "UpdateVoter failed; retrying"
                ),
            }
        }
        tokio::time::sleep(params.retry_backoff.to_std()).await;
    }
}
