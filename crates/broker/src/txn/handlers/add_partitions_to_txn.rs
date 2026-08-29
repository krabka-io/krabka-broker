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
//! ## ACL preamble
//!
//! For each transaction in the request:
//! * `Write` on `TransactionalId(tid)`. On a deny, every topic row in that
//!   transaction's results emits `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`
//!   on every partition.
//! * For each topic, `Write` on `Topic(name)`. On a deny, that topic's
//!   partition rows emit `TOPIC_AUTHORIZATION_FAILED (29)`.

use bytes::Bytes;
use krabka_protocol::{Decode, owned::add_partitions_to_txn_request::AddPartitionsToTxnRequest};

mod authz;
mod registration;
mod results;
mod versions;
mod wire;

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
    };
    if version >= 4 {
        handle_v4(&dependencies, version, &req).await
    } else {
        handle_v3(&dependencies, version, &req).await
    }
}
