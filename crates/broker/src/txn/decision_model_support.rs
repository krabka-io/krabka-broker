//! Shared initialization transitions and independent Kafka fence oracle for the
//! transaction decision and timeout models.

use krabka_log::ProducerId;

use super::{
    coordinator::completion::completion_for,
    handlers::end_txn::{completion_producer_identity, prepare_server_abort_identities_with_fresh},
    state::{TxnEntry, TxnState},
    version::TxnVersion,
};

pub struct Initialized {
    pub entry: TxnEntry,
    pub fence_matches: bool,
    pub reset: bool,
}

/// Kafka's coordinator abort, restated from `TransactionMetadata` rather than
/// read back from the production function. The epoch advances once; `TV_2`
/// records the held epoch and version, while earlier versions clear both.
pub fn fenced_as_kafka(entry: &TxnEntry, held: i16, version: TxnVersion) -> bool {
    let verified = version == TxnVersion::Verified;
    entry.producer_epoch == held + 1
        && entry.last_producer_epoch == if verified { held } else { -1 }
        && entry.client_transaction_version == if verified { 2 } else { 0 }
        && completion_producer_identity(entry) == (entry.producer_id, held + 1)
}

/// Drive the handler's prepared-state gate, ongoing fence and terminal reset.
/// An ongoing fence retains its previous timeout until completion and retry.
pub fn initialize(
    mut entry: TxnEntry,
    version: TxnVersion,
    timeout_ms: i32,
    now_ms: i64,
) -> Option<Initialized> {
    if completion_for(entry.state).is_some() {
        return None;
    }
    let reset = entry.state != TxnState::Ongoing;
    let fence_matches = if reset {
        let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
            true,
            false,
            entry.producer_id.get(),
            entry.producer_epoch,
            None,
        )
        .expect("model epochs never reach the rotation boundary");
        entry = TxnEntry::new_empty(
            "tid".to_string(),
            ProducerId(pid),
            epoch,
            timeout_ms,
            now_ms,
        );
        true
    } else {
        let held = entry.producer_epoch;
        entry.state = TxnState::PrepareAbort;
        prepare_server_abort_identities_with_fresh(&mut entry, version, None)
            .expect("model epochs never reach the rotation boundary");
        fenced_as_kafka(&entry, held, version)
    };
    Some(Initialized {
        entry,
        fence_matches,
        reset,
    })
}
