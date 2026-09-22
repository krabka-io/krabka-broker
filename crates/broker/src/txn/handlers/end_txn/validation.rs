//! The checks `EndTxn` runs before it touches the transaction: the ACL on the
//! transactional id, coordinator ownership, and the producer identity and state
//! the coordinator entry holds. The outcome is either an entry to finalise, the
//! already-complete answer for an idempotent retry, or a Kafka error code.

use krabka_log::ProducerId;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::end_txn_request::EndTxnRequest;

use super::producer_identity::client_producer_identity;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
    txn::state::{TxnEntry, TxnState},
};

pub(super) enum EndTxnValidation {
    Proceed(std::sync::Arc<tokio::sync::Mutex<TxnEntry>>),
    AlreadyComplete(ProducerId, i16),
}

pub(super) async fn validate_end_txn(
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &EndTxnRequest,
    version: crate::txn::version::TxnVersion,
) -> Result<EndTxnValidation, i16> {
    let transactional_id = request.transactional_id.as_str();
    let authorization = AuthorizationRequest {
        principal: context.principal,
        host: context.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: transactional_id,
        operation: AclOperation::Write,
    };
    if authorizer.authorize(image, &authorization) == AuthorizationResult::Deny {
        return Err(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    if let Some(code) = coordinator.coordinator_error(transactional_id).await {
        return Err(code);
    }
    // A leadership change can unload this partition between the check above
    // and the lookup below. Recheck the coordinator status before answering
    // INVALID_PRODUCER_ID_MAPPING, so a client retries a failover with the
    // retriable COORDINATOR_LOAD_IN_PROGRESS or NOT_COORDINATOR instead of
    // seeing its valid transaction reported as unknown.
    let Some(entry) = coordinator.get(transactional_id) else {
        return Err(missing_entry_error(
            coordinator.coordinator_error(transactional_id).await,
        ));
    };
    {
        let state = entry.lock().await;
        if matches!(
            state.state,
            TxnState::PrepareCommit | TxnState::PrepareAbort
        ) {
            return Err(codes::CONCURRENT_TRANSACTIONS);
        }
        let request_pid = ProducerId(request.producer_id);
        let request_epoch = request.producer_epoch;
        if matches!(
            state.state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            let same_result = matches!(state.state, TxnState::CompleteCommit) == request.committed;
            if same_result
                && is_completed_end_txn_retry(
                    &state,
                    request_pid,
                    request_epoch,
                    version.verified(),
                )
            {
                return Ok(EndTxnValidation::AlreadyComplete(
                    state.producer_id,
                    state.producer_epoch,
                ));
            }
        }
        if client_producer_identity(&state) != (request_pid, request_epoch) {
            return Err(codes::INVALID_PRODUCER_EPOCH);
        }
    }
    Ok(EndTxnValidation::Proceed(entry))
}

/// The error `EndTxn` answers when the coordinator holds no entry for the
/// requested transactional id.
///
/// A leadership change can unload the coordinator partition, and so evict its
/// entries, between the `coordinator_error` check above and the lookup that
/// misses. `recheck` is a fresh read of the coordinator status taken after
/// that miss: when it still names an error, the miss is the unload, and the
/// caller answers the retriable `COORDINATOR_LOAD_IN_PROGRESS` or
/// `NOT_COORDINATOR` instead of `INVALID_PRODUCER_ID_MAPPING`, so a client
/// retries the failover instead of seeing a valid transaction reported as
/// unknown.
fn missing_entry_error(recheck: Option<i16>) -> i16 {
    recheck.unwrap_or(codes::INVALID_PRODUCER_ID_MAPPING)
}

/// Whether a request for a completed transaction is a retry of the `EndTxn`
/// that completed it.
///
/// With the epoch bump of transaction version 2, the retry carries the epoch
/// before the bump, or the last epoch of the prior producer ID after a
/// rotation (Kafka `endTransaction`, `retryOnEpochBump` and
/// `retryOnOverflow`). Below version 2 the completion keeps the identity, and
/// Kafka's `endTransactionWithTV1` answers `NONE` to the same identity and
/// result.
fn is_completed_end_txn_retry(
    entry: &TxnEntry,
    request_pid: ProducerId,
    request_epoch: i16,
    completion_bumps_epoch: bool,
) -> bool {
    if !completion_bumps_epoch {
        return entry.producer_id == request_pid && entry.producer_epoch == request_epoch;
    }
    (entry.producer_id == request_pid && request_epoch.checked_add(1) == Some(entry.producer_epoch))
        || (entry.prev_producer_id == request_pid
            && request_epoch == i16::MAX - 1
            && entry.producer_epoch == 0)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn a_completed_retry_depends_on_the_epoch_bump() {
        let mut bumped = TxnEntry::new_empty("tid".to_owned(), ProducerId(7), 4, 60_000, 0);
        bumped.state = TxnState::CompleteCommit;
        let mut rotated = bumped.clone();
        rotated.producer_id = ProducerId(9);
        rotated.producer_epoch = 0;
        rotated.prev_producer_id = ProducerId(7);

        // (label, entry, request identity, completion bumps the epoch, retry)
        let cases = [
            ("bump, epoch before the bump", &bumped, (7, 3), true, true),
            ("bump, epoch after the bump", &bumped, (7, 4), true, false),
            ("bump, other producer", &bumped, (8, 3), true, false),
            (
                "rotation, prior producer",
                &rotated,
                (7, i16::MAX - 1),
                true,
                true,
            ),
            (
                "rotation, prior producer early epoch",
                &rotated,
                (7, 3),
                true,
                false,
            ),
            ("no bump, same identity", &bumped, (7, 4), false, true),
            ("no bump, older epoch", &bumped, (7, 3), false, false),
            ("no bump, other producer", &bumped, (8, 4), false, false),
        ];
        for (label, entry, (pid, epoch), bumps, retry) in cases {
            check!(
                is_completed_end_txn_retry(entry, ProducerId(pid), epoch, bumps) == retry,
                "{label}"
            );
        }
    }

    /// Regression: a leadership change can unload the coordinator partition
    /// between the `coordinator_error` check and the `get` lookup. Before the
    /// fix, a missing entry always answered `INVALID_PRODUCER_ID_MAPPING`,
    /// even when a fresh coordinator-status read named a retriable error.
    #[test]
    fn a_missing_entry_prefers_a_fresh_coordinator_error_over_invalid_mapping() {
        // (label, fresh recheck, answer)
        let cases = [
            (
                "healthy coordinator, unknown id",
                None,
                codes::INVALID_PRODUCER_ID_MAPPING,
            ),
            (
                "partition unloaded mid-race",
                Some(codes::NOT_COORDINATOR),
                codes::NOT_COORDINATOR,
            ),
            (
                "partition still loading mid-race",
                Some(codes::COORDINATOR_LOAD_IN_PROGRESS),
                codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
        ];
        for (label, recheck, answer) in cases {
            check!(missing_entry_error(recheck) == answer, "{label}");
        }
    }
}
