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
    if !coordinator.is_coordinator_for(transactional_id).await {
        return Err(codes::NOT_COORDINATOR);
    }
    let entry = coordinator
        .get(transactional_id)
        .ok_or(codes::INVALID_PRODUCER_ID_MAPPING)?;
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
            if same_result && is_completed_end_txn_retry(&state, request_pid, request_epoch) {
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

fn is_completed_end_txn_retry(
    entry: &TxnEntry,
    request_pid: ProducerId,
    request_epoch: i16,
) -> bool {
    (entry.producer_id == request_pid && request_epoch.checked_add(1) == Some(entry.producer_epoch))
        || (entry.prev_producer_id == request_pid
            && request_epoch == i16::MAX - 1
            && entry.producer_epoch == 0)
}
