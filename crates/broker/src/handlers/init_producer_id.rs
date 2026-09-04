//! `InitProducerId` (`api_key=22`). This handler hands out
//! `(producer_id, producer_epoch)` to a producer, or it initialises /
//! re-initialises a transactional producer.
//!
//! Non-transactional path: idempotent-producer support.
//! Transactional path:     coordinator routing.
//!
//! ## ACL preamble
//!
//! Two distinct authorize gates branch off `req.transactional_id`:
//!
//! * `Some(non-empty)` → `Write` on
//!   `TransactionalId(transactional_id)`. Deny →
//!   `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `None | Some("")` (idempotent-only producer) →
//!   `IdempotentWrite` on `Cluster("kafka-cluster")`. Deny →
//!   `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
    },
};
use krabka_units::convert::TimeExt as _;

mod identity;
mod transactional;

#[cfg(test)]
mod tests;

use self::transactional::handle_transactional;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    replicator_supervisor::materialize_partition,
};

#[tracing::instrument(
    name = "handle_init_producer_id",
    level = "info",
    skip_all,
    fields(api = "InitProducerId", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let producer_ids = broker.producer_ids.clone();
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();

    let mut cur: &[u8] = req_bytes;
    let req = InitProducerIdRequest::decode(&mut cur, version)?;

    // ── ACL preamble ────────────────────────────────────────
    // Branch on whether this is an idempotent-only or transactional
    // request and gate on the appropriate resource/operation.
    {
        let image = controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        match req.transactional_id.as_deref() {
            Some(tid) if !tid.is_empty() => {
                let acl_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::Write,
                };
                if authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
            }
            _ => {
                let acl_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                    operation: AclOperation::IdempotentWrite,
                };
                if authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::CLUSTER_AUTHORIZATION_FAILED);
                }
            }
        }
    }

    let resp = match req.transactional_id.as_deref() {
        None | Some("") => {
            // Non-transactional path (idempotence).
            let (pid, epoch) = producer_ids.allocate().await?;
            InitProducerIdResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                producer_id: pid.get(),
                producer_epoch: epoch,
                ..Default::default()
            }
        }
        Some(tid) => {
            // Refresh the coordinator's leader-partition view from the
            // current metadata image. This is a cheap idempotent read,
            // and it ensures we don't race with the replicator-supervisor
            // loop when a `FindCoordinator(TRANSACTION)` call that
            // triggered `__transaction_state` bootstrap just happened.
            let image = controller.current_image();
            let txnv = crate::txn::version::resolve_txn_version(&image);

            // ── KIP-939 two-phase-commit gates ───────────────────────────
            // Validated up-front (like Kafka's `handleInitProducerId`), before
            // the coordinator-ness check, so a client learns its request is
            // unauthorized / unsupported regardless of which broker it hit.
            if req.enable2_pc || req.keep_prepared_txn {
                // (1) Cluster must have 2PC enabled. Kafka maps a disabled
                //     cluster to TRANSACTIONAL_ID_AUTHORIZATION_FAILED (not an
                //     UNSUPPORTED_*), so a client can't probe the feature flag.
                if !broker.config.features.transaction_two_phase_commit_enable {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
                // (2) Principal must hold the TWO_PHASE_COMMIT ACL on the tid,
                //     in addition to the Write checked in the preamble.
                let two_pc_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::TwoPhaseCommit,
                };
                if broker.config.authorizer.authorize(&*image, &two_pc_req)
                    == AuthorizationResult::Deny
                {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
            }
            if req.keep_prepared_txn && (req.producer_id != -1 || req.producer_epoch != -1) {
                return encode_err(version, codes::INVALID_REQUEST);
            }
            if (req.enable2_pc || req.keep_prepared_txn) && !txnv.two_phase() {
                return encode_err(version, codes::UNSUPPORTED_VERSION);
            }
            coord.refresh_leader_partitions(&image).await;

            // Verify we're the coordinator for this tid.
            if coord.is_coordinator_for(tid).await {
                // Ensure the __transaction_state partition for this tid
                // is materialized on disk. The replicator-supervisor
                // handles this asynchronously, but we may race with it
                // when FindCoordinator just bootstrapped the topic in
                // the same request round-trip. `materialize_partition`
                // uses `DashMap::entry()` to atomically check-and-insert,
                // so two concurrent InitProducerId calls for the same
                // partition cannot both spawn independent writer tasks.
                let txn_partition = coord.partition_for(tid);
                materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                    partitions: &coord.partitions,
                    topic: crate::txn::bootstrap::TOPIC,
                    topic_id: None,
                    partition: txn_partition.get(),
                    log_dirs: &log_dirs,
                    log_config: &log_config,
                    log_dir_status: &log_dir_status,
                    producer_state: &broker.producer_state,
                    producer_id_expiration: broker.config.producer_id_expiration,
                    max_produce_group: broker.config.max_produce_group,
                    partition_writer_queue_depth: broker.config.partition_writer_queue_depth,
                    diskless_wal_local_replica_count: broker
                        .config
                        .diskless_wal_local_replica_count,
                    diskless: false,
                    hot_tail: None,
                    wal_shards: None,
                    sequencer: None,
                })
                .map_err(BrokerError::Txn)?;
                let txn_timeout = crate::txn::two_pc::resolve_txn_timeout(
                    req.enable2_pc,
                    req.transaction_timeout_ms,
                    broker.config.transaction_min_timeout.millis_i32(),
                    broker.config.transaction_max_timeout.millis_i32(),
                );
                handle_transactional(
                    &coord,
                    tid,
                    txnv,
                    txn_timeout,
                    req.enable2_pc,
                    req.keep_prepared_txn,
                    // KIP-360: the identity the caller believes it holds. It
                    // is `(-1, -1)` below v3 and for a first initialisation.
                    (req.producer_id, req.producer_epoch),
                )
                .await?
            } else {
                InitProducerIdResponse {
                    error_code: codes::NOT_COORDINATOR,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
            }
        }
    };

    crate::handlers::encode_response(&resp, version)
}

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
