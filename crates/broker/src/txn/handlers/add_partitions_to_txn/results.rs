//! The per-topic and per-partition result rows of an `AddPartitionsToTxn`
//! response.
//!
//! Every path through the handler answers with the same nested row shape, so
//! the three builders here are the single place that decides which error code
//! lands on which partition row: one shared code for the whole transaction,
//! one shared code with the per-topic refusal in
//! [`topic_refusal`](super::write_freeze::topic_refusal) overriding it, and
//! the KIP-890 verify-only shape whose code is per partition.

use krabka_ids::PartitionIndex;
use krabka_protocol::owned::common::{
    add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
        add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};

use super::write_freeze::topic_refusal;
use crate::{codes, txn::state::TopicPartition};

/// KIP-890 `TV_2` verify-only per-partition decision. It gives `NONE (0)` when
/// the partition is already part of the ongoing transaction, and
/// `TRANSACTION_ABORTABLE (120)` in every other case. This matches the
/// verify-only path in cp-kafka 4.0:
/// `if txnMetadata.topicPartitions.contains(part) NONE else TRANSACTION_ABORTABLE`.
fn verify_partition_code(entry: &crate::txn::state::TxnEntry, tp: &TopicPartition) -> i16 {
    if entry.partitions.contains(tp) {
        codes::NONE
    } else {
        codes::TRANSACTION_ABORTABLE
    }
}

/// Builds the verify-only response. It has the same shape as
/// `per_topic_with_refusals` on the add path, but each partition carries its
/// own verify result instead of one shared code. A denied or frozen topic
/// still short-circuits to its refusal on every partition row.
pub(super) fn verify_partitions(
    entry: &crate::txn::state::TxnEntry,
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let refusal = topic_refusal(&t.name, denied, frozen);
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| {
                        let row_code = refusal.unwrap_or_else(|| {
                            verify_partition_code(
                                entry,
                                &TopicPartition {
                                    topic: t.name.clone(),
                                    partition: PartitionIndex(p),
                                },
                            )
                        });
                        AddPartitionsToTxnPartitionResult {
                            partition_index: p,
                            partition_error_code: row_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Builds a per-topic and per-partition result list. A topic named in `denied`
/// gets `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row, and one
/// named in `frozen` gets `POLICY_VIOLATION (44)`. Every other topic gets
/// `code`.
pub(super) fn per_topic_with_refusals(
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let row_code = topic_refusal(&t.name, denied, frozen).unwrap_or(code);
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| AddPartitionsToTxnPartitionResult {
                        partition_index: p,
                        partition_error_code: row_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Builds a per-topic and per-partition result list in which every partition
/// carries `error_code`. Whole-transaction errors use it, such as the txn-id
/// ACL deny path.
pub(super) fn topic_error(
    topics: &[AddPartitionsToTxnTopic],
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| AddPartitionsToTxnTopicResult {
            name: t.name.clone(),
            results_by_partition: t
                .partitions
                .iter()
                .map(|&p| AddPartitionsToTxnPartitionResult {
                    partition_index: p,
                    partition_error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::assert;

    use super::*;
    use crate::txn::{
        handlers::add_partitions_to_txn::test_support::{topic, topic_result},
        state::TxnEntry,
    };

    #[test]
    fn verify_only_codes_present_vs_absent() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        let present = TopicPartition {
            topic: "a".into(),
            partition: PartitionIndex(0),
        };
        e.partitions.insert(present.clone());
        let absent = TopicPartition {
            topic: "b".into(),
            partition: PartitionIndex(0),
        };
        assert!(verify_partition_code(&e, &present) == codes::NONE);
        assert!(verify_partition_code(&e, &absent) == codes::TRANSACTION_ABORTABLE);
    }

    #[test]
    fn verify_partitions_preserves_topic_and_partition_rows() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        e.partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: PartitionIndex(1),
        });
        let topics = vec![topic("alpha", &[1, 2]), topic("denied", &[3])];
        let denied = maplit::hashset! {"denied".to_string()};

        let frozen = maplit::hashset! {"frozen".to_string()};
        let topics = [topics, vec![topic("frozen", &[4])]].concat();

        let rows = verify_partitions(&e, &topics, &denied, &frozen);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NONE), (2, codes::TRANSACTION_ABORTABLE)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(4, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn per_topic_with_refusals_preserves_rows_and_overrides_refused_topics() {
        let topics = vec![
            topic("alpha", &[1, 2]),
            topic("denied", &[3]),
            topic("frozen", &[4]),
        ];
        let denied = maplit::hashset! {"denied".to_string()};
        let frozen = maplit::hashset! {"frozen".to_string()};

        let rows = per_topic_with_refusals(&topics, &denied, &frozen, codes::NOT_COORDINATOR);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NOT_COORDINATOR), (2, codes::NOT_COORDINATOR)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(4, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn topic_error_preserves_each_requested_partition() {
        let topics = vec![topic("alpha", &[4, 5])];

        let rows = topic_error(&topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

        let expected = vec![topic_result(
            "alpha",
            &[
                (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                (5, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            ],
        )];
        assert!(rows == expected);
    }
}
