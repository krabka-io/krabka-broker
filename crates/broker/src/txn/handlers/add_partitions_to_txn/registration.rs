//! The per-transaction half of `AddPartitionsToTxn`, shared by both wire
//! versions.
//!
//! Once the ACL gates have run, every version path faces the same sequence:
//! confirm this broker coordinates the `transactional_id`, look the entry up,
//! answer a KIP-890 verify-only request from the partitions already in the
//! transaction, and otherwise hand the requested partitions to the coordinator
//! for registration.

use krabka_ids::PartitionIndex;
use krabka_protocol::owned::common::{
    add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
};

use super::results::{per_topic_with_refusals, verify_partitions};
use crate::{codes, txn::state::TopicPartition};

/// Processes one `transactional_id`, `producer_id`, and `producer_epoch`
/// triple. It returns per-topic and per-partition result entries. A topic
/// named in `denied` short-circuits with `TOPIC_AUTHORIZATION_FAILED`, and one
/// named in `frozen` with `POLICY_VIOLATION`. Every other topic goes through
/// the state-machine check and the partition registration.
pub(super) struct TransactionRequest<'a> {
    pub(super) transactional_id: &'a str,
    pub(super) producer_id: krabka_log::ProducerId,
    pub(super) producer_epoch: i16,
    pub(super) topics: &'a [AddPartitionsToTxnTopic],
    pub(super) denied: &'a std::collections::HashSet<String>,
    pub(super) frozen: &'a std::collections::HashSet<String>,
    pub(super) txnv: crate::txn::version::TxnVersion,
    pub(super) verify_only: bool,
}

// cargo-mutants: orchestration over live coordinator state. Every branch is a
// call into a kernel that is mutation-tested on its own -- the ACL/freeze
// short-circuits, `krabka_verified::transaction`'s state-machine check, and
// `TxnEntry`'s partition registration -- and what remains here is the loop that
// walks the request's topics and locks the entry.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn process_one_txn(
    coord: &crate::txn::coordinator::TxnCoordinator,
    request: TransactionRequest<'_>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let TransactionRequest {
        transactional_id: tid,
        producer_id,
        producer_epoch,
        topics,
        denied,
        frozen,
        txnv,
        verify_only,
    } = request;
    // Topics allowed to proceed past the per-topic Write ACL gate and the
    // write-freeze gate. A frozen topic never joins the partition set, which
    // is what keeps the transaction from ever reaching its log.
    let allowed_topics: Vec<&AddPartitionsToTxnTopic> = topics
        .iter()
        .filter(|t| !denied.contains(&t.name) && !frozen.contains(&t.name))
        .collect();

    // 1. Coordinator check (applies only to non-denied topics — for
    //    denied topics we always emit TOPIC_AUTHORIZATION_FAILED).
    //
    //    It runs ahead of the freeze gate, so this path passes no freeze set
    //    down. A client that reached the wrong broker has to learn that first:
    //    it then retries at the real coordinator, which is the broker that
    //    owns the decision and answers the freeze.
    if !coord.is_coordinator_for(tid).await {
        let unread = std::collections::HashSet::new();
        return per_topic_with_refusals(topics, denied, &unread, codes::NOT_COORDINATOR);
    }

    // 2. Look up entry for the TV_2 verify-only path.
    let Some(entry_mutex) = coord.get(tid) else {
        return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_PRODUCER_ID_MAPPING);
    };
    if txnv.verified() && verify_only {
        let entry = entry_mutex.lock().await;
        if entry.has_staged_producer_identity() {
            return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_TXN_STATE);
        }
        if entry.producer_id != producer_id || entry.producer_epoch != producer_epoch {
            return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_PRODUCER_EPOCH);
        }
        return verify_partitions(&entry, topics, denied, frozen);
    }
    drop(entry_mutex);

    let partitions = allowed_topics
        .into_iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|&partition| TopicPartition {
                topic: topic.name.clone(),
                partition: PartitionIndex(partition),
            })
        })
        .collect();
    let code = coord
        .register_partitions(tid, producer_id, producer_epoch, partitions, txnv)
        .await;
    per_topic_with_refusals(topics, denied, frozen, code)
}
