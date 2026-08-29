//! `BrokerHeartbeat` (`api_key=63`). KIP-500 controller-side heartbeat handler.
//!
//! Only the openraft leader handles heartbeats. Non-leaders return
//! `NOT_CONTROLLER` so the broker client can redirect.
//!
//! This file holds the wire handler and the order its stages run in. The
//! `ClusterAction` gate lives in `authorization`, the leadership, registration
//! and offline-dir gates in `validation`, the response bodies in `response`,
//! the controlled-shutdown drain in `shutdown`, and the KIP-112 offline-dir
//! failover in `failover`.

use bytes::Bytes;
use krabka_protocol::{Decode, owned::broker_heartbeat_request::BrokerHeartbeatRequest};
use krabka_raft::NodeId;

mod authorization;
mod failover;
mod response;
mod shutdown;
mod validation;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::failover::failover_offline_dirs;
use self::{
    authorization::{cluster_action_denied, denied_response},
    response::{encode_response, error_response, not_controller_response, success_response},
    shutdown::drain_leaderships_for_shutdown,
    validation::{has_offline_log_dirs, is_controller_leader, validate_registration},
};
use crate::{broker::Broker, error::BrokerError};

#[tracing::instrument(
    name = "handle_broker_heartbeat",
    level = "info",
    skip_all,
    fields(api = "BrokerHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let liveness = broker.liveness.clone();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let metrics = broker.metrics.clone();
    let recovery = broker.unclean_recovery.clone();
    // Check leadership: this broker is the controller leader iff the
    // watch channel reports a leader id equal to our own node_id.
    let is_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| is_controller_leader(Some(n), node_id));
    {
        let mut cur: &[u8] = req_bytes;
        let req = BrokerHeartbeatRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = controller.current_image();
            if cluster_action_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        // Only the openraft leader handles heartbeats. NOT_CONTROLLER
        // tells the broker client to redirect.
        if !is_leader {
            return encode_response(version, &not_controller_response());
        }

        let image = controller.current_image();
        let (broker_id_u64, is_caught_up) = match validate_registration(&image, &req) {
            Ok(validated) => validated,
            Err(error_code) => return encode_response(version, &error_response(error_code)),
        };

        // Record the heartbeat. If it's a revival, the liveness ticker
        // will pick up the transition next cycle and the heartbeat-side
        // wakeup is a no-op; the controlled-shutdown path handles
        // explicit on-revival handling.
        let _transition = liveness.record_fenced_heartbeat(broker_id_u64).await;
        let is_fenced = liveness
            .apply_fencing(broker_id_u64, req.want_fence, is_caught_up)
            .await;

        // Track want_shut_down state and drive leader transfer.
        liveness
            .set_wants_shutdown(broker_id_u64, req.want_shut_down)
            .await;

        let should_shut_down = if req.want_shut_down {
            drain_leaderships_for_shutdown(&controller, &liveness, NodeId(broker_id_u64)).await?
        } else {
            false
        };

        // KIP-112: a broker that reports offline log dirs is still alive, so
        // the liveness `alive→dead` failover never fires. Map the reported
        // offline dir UUIDs to the reporting broker's affected partitions and
        // fail them over (elect from surviving alive ISR, drop the offline
        // replica). Only the controller leader reaches here (NOT_CONTROLLER
        // early-return above), and it's idempotent across repeated heartbeats.
        //
        // Validate the reporting broker id independently of `broker_id_u64`
        // (which falls back to 0 for the liveness path): failing over the
        // wrong broker on a malformed negative id would be harmful.
        if has_offline_log_dirs(&req)
            && let Ok(reporting_broker) = u64::try_from(req.broker_id)
        {
            let offline: std::collections::HashSet<uuid::Uuid> = req
                .offline_log_dirs
                .iter()
                .map(|u| uuid::Uuid::from_bytes(u.0))
                .collect();
            let recoveries = failover_offline_dirs(
                &controller,
                NodeId(reporting_broker),
                &offline,
                &liveness,
                &metrics,
            )
            .await;
            // Fire-and-forget: enqueue logs internally if the recovery manager is gone.
            for (topic, partition, strategy) in recoveries {
                recovery
                    .enqueue(crate::unclean_recovery::RecoveryJob {
                        topic,
                        partition,
                        strategy,
                        reply: None,
                        // KFC-9: a heartbeat that reports an offline log dir
                        // is not a request for an unclean recovery, and the
                        // broker that sent it is not a person who can be asked
                        // for a second signature. The recovery carries no
                        // proposal, and
                        // `break_glass.background_unclean_recovery` decides
                        // what the URM does with it.
                        proposal: None,
                    })
                    .await;
            }
        }

        encode_response(
            version,
            &success_response(is_caught_up, is_fenced, should_shut_down),
        )
    }
}
