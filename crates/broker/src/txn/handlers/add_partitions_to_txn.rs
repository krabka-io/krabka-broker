//! `AddPartitionsToTxn` (`api_key=24`). It registers one or more
//! (topic, partition) pairs with an ongoing transaction.
//!
//! Wire-format versions:
//!  - v0-3: one `(transactional_id, producer_id, producer_epoch, topics)` on
//!    the request, and `results_by_topic_v3_and_below` on the response.
//!  - v4-5: a batched `transactions` array on the request, and
//!    `results_by_transaction` on the response.
//!
//! This broker handles only the single-tid case, which is the only shape a
//! producer client ever sends. When a v4+ request carries more than one
//! transaction entry, the handler processes them all in sequence.
//!
//! ## Authorization
//!
//! The checks are Kafka's `KafkaApis.handleAddPartitionsToTxnRequest`:
//! * v4 and later come from brokers. The whole request needs `ClusterAction`
//!   on the cluster, and a deny answers a top-level
//!   `CLUSTER_AUTHORIZATION_FAILED (31)`. No transactional id or topic ACL is
//!   checked.
//! * v0 to v3 come from clients. `Write` on `TransactionalId(tid)`, else every
//!   partition answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`. Then
//!   `Write` on each non-internal topic. An internal topic is never
//!   authorized.
//! * For every version, a partition is `TOPIC_AUTHORIZATION_FAILED (29)` when
//!   its topic is not authorized, else `UNKNOWN_TOPIC_OR_PARTITION (3)` when
//!   the metadata image has no such partition. Any such partition fails the
//!   whole transaction: nothing is added, and every other partition answers
//!   `OPERATION_NOT_ATTEMPTED (55)`.
//!
//! ## Write-freeze gate
//!
//! A topic that a KFC-9 write freeze covers never joins the transaction's
//! partition set, and every one of its partition rows emits
//! `POLICY_VIOLATION (44)`. This is the cheapest place to stop a transaction
//! from ever reaching a frozen topic.
//!
//! A producer that enlisted the partition before the freeze landed keeps its
//! ability to commit or abort. The gate refuses the *next* enlistment, and a
//! freeze never stops an open transaction from completing.
//!
//! The gate runs after both ACL checks and after the coordinator check. A
//! caller learns that it is unauthorized, or that it reached the wrong broker,
//! before it learns anything about the topic's freeze state.

use bytes::Bytes;
use krabka_protocol::{Decode, owned::add_partitions_to_txn_request::AddPartitionsToTxnRequest};

mod authz;
mod registration;
mod results;
mod versions;
mod wire;
mod write_freeze;

#[cfg(test)]
mod authorization_tests;
#[cfg(test)]
mod test_support;

use self::versions::{HandlerDependencies, handle_v3, handle_v4};
use crate::{broker::Broker, error::BrokerError};

#[tracing::instrument(
    name = "handle_add_partitions_to_txn",
    level = "info",
    skip_all,
    fields(api = "AddPartitionsToTxn", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let authorizer = broker.config.authorizer.as_ref();
    let mut cur: &[u8] = req_bytes;
    let req = AddPartitionsToTxnRequest::decode(&mut cur, version)?;

    // Versions 4 and later come only from brokers. A deny is
    // `AddPartitionsToTxnRequest.getErrorResponse`: the top-level error code.
    if version >= 4
        && crate::handlers::cluster_action_denied(authorizer, &controller.current_image(), ctx)
    {
        return wire::encode_response(
            &krabka_protocol::owned::add_partitions_to_txn_response::AddPartitionsToTxnResponse {
                error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
            version,
        );
    }

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness, to avoid a race.
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coord.refresh_leader_partitions(&image).await;

    let dependencies = HandlerDependencies {
        coord: &coord,
        image: &image,
        txnv,
        authorizer,
        principal: ctx.principal,
        peer: ctx.peer,
        config: &broker.config,
    };
    if version >= 4 {
        handle_v4(&dependencies, version, &req).await
    } else {
        handle_v3(&dependencies, version, &req).await
    }
}
