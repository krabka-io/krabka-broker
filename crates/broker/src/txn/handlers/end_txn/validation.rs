//! The checks `EndTxn` runs before it touches the transaction: the ACL on the
//! transactional id, coordinator ownership, and the producer identity and state
//! the coordinator entry holds. The outcome is either an entry to finalise, the
//! already-complete answer for an idempotent retry, or a Kafka error code.

use krabka_log::ProducerId;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::end_txn_request::EndTxnRequest;

use super::state_table::{EndTxnDecision, end_txn_decision};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
    txn::state::TxnEntry,
};

pub(super) enum EndTxnValidation {
    /// Finalise the transaction. `no_partition_added` marks Kafka's
    /// `prepareAbortOrCommit(..., noPartitionAdded = true)`.
    Proceed {
        entry: std::sync::Arc<tokio::sync::Mutex<TxnEntry>>,
        no_partition_added: bool,
    },
    /// Answer `NONE` with this identity and write nothing.
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
    // Kafka `endTransaction` refuses an empty transactional id before it looks
    // the coordinator up.
    if transactional_id.is_empty() {
        return Err(codes::INVALID_REQUEST);
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
    let decision = {
        let state = entry.lock().await;
        end_txn_decision(
            &state,
            (ProducerId(request.producer_id), request.producer_epoch),
            request.committed,
            version.verified(),
        )
    };
    match decision {
        EndTxnDecision::Prepare {
            no_partition_added, ..
        } => Ok(EndTxnValidation::Proceed {
            entry,
            no_partition_added,
        }),
        EndTxnDecision::AlreadyComplete => {
            let state = entry.lock().await;
            Ok(EndTxnValidation::AlreadyComplete(
                state.producer_id,
                state.producer_epoch,
            ))
        }
        EndTxnDecision::Refuse(code) => Err(code),
    }
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

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
