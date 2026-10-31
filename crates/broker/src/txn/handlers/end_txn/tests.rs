//! `EndTxn` Phase 3 against a running coordinator: the `Complete*` record it
//! persists.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        codes,
        test_support::{
            decode_response, dispatch_context, encode_request, peer, principal, request_context,
        },
        txn::{
            handlers::add_partitions_to_txn::test_support::{CoordinatorBroker, start_coordinator},
            state::{TxnEntry, TxnState},
            version::resolve_txn_version,
        },
    },
    assert2::check,
    krabka_log::ProducerId,
    krabka_protocol::owned::{
        end_txn_request::{self, EndTxnRequest},
        end_txn_response::EndTxnResponse,
    },
    std::sync::Arc,
};

const TID: &str = "tid-staged-recovery";
/// The producer id that wrote the transaction.
const LIVE_PID: ProducerId = ProducerId(11);
/// The KIP-939 recovery identity staged on the open transaction, which the
/// client holds and names in `EndTxn`.
const STAGED_PID: ProducerId = ProducerId(21);

/// The identity fields of the persisted `Complete*` entry.
#[derive(Debug, PartialEq, Eq)]
struct CompletedIdentity {
    error_code: i16,
    state: TxnState,
    producer_id: ProducerId,
    producer_epoch: i16,
    prev_producer_id: ProducerId,
    next_producer_id: ProducerId,
    next_producer_epoch: i16,
}

/// Kafka's `prepareComplete` records the metadata's own `producerId` as the
/// previous producer id (`prepareTransitionTo` passes `this.producerId`), and
/// the reaper and the completion task follow it through `apply_completion`.
/// The request names the staged recovery identity, so it must not be the
/// source of `prev_producer_id`.
#[yare::parameterized(
    staged_identity_continues = { 3, false },
    staged_identity_rotates_at_the_epoch_boundary = { i16::MAX, true },
)]
#[test_macro(tokio::test)]
async fn phase3_records_the_live_producer_id_as_previous(staged_epoch: i16, rotates: bool) {
    let CoordinatorBroker { handle, dir: _dir } =
        start_coordinator(Arc::new(AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    let mut open = TxnEntry::new_empty(TID.to_owned(), LIVE_PID, 5, 30_000, 0);
    open.state = TxnState::Ongoing;
    open.next_producer_id = STAGED_PID;
    open.next_producer_epoch = staged_epoch;
    broker
        .txn_coordinator
        .put(
            open,
            resolve_txn_version(&broker.controller.current_image()),
        )
        .await
        .expect("seed the open transaction");

    let version = 5;
    let request = EndTxnRequest {
        transactional_id: TID.into(),
        producer_id: STAGED_PID.get(),
        producer_epoch: staged_epoch,
        committed: true,
        ..Default::default()
    };
    let user = principal("ANONYMOUS");
    let address = peer();
    let ctx = request_context(&user, &address, "end-txn-phase3");
    let bytes = dispatch_context(
        &broker,
        end_txn_request::API_KEY,
        version,
        &encode_request(&request, version),
        &ctx,
    )
    .await;
    let response: EndTxnResponse = decode_response(&bytes, version);
    let completed = broker
        .txn_coordinator
        .get(TID)
        .expect("the transaction")
        .lock()
        .await
        .clone();

    // A rotation hands out a freshly allocated producer id at epoch 0.
    let (completion_pid, completion_epoch) = if rotates {
        check!(response.producer_id != LIVE_PID.get());
        check!(response.producer_id != STAGED_PID.get());
        (ProducerId(response.producer_id), 0)
    } else {
        (STAGED_PID, staged_epoch + 1)
    };
    let actual = CompletedIdentity {
        error_code: response.error_code,
        state: completed.state,
        producer_id: completed.producer_id,
        producer_epoch: completed.producer_epoch,
        prev_producer_id: completed.prev_producer_id,
        next_producer_id: completed.next_producer_id,
        next_producer_epoch: completed.next_producer_epoch,
    };
    let expected = CompletedIdentity {
        error_code: codes::NONE,
        state: TxnState::CompleteCommit,
        producer_id: completion_pid,
        producer_epoch: completion_epoch,
        prev_producer_id: LIVE_PID,
        next_producer_id: ProducerId(-1),
        next_producer_epoch: -1,
    };
    check!(actual == expected);
}
