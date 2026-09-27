//! The values that the `ShareFetch` handler derives straight from a decoded
//! `ShareFetchRequest`, before it reaches any share-partition state.
//!
//! The share-session update needs to know whether the request carried
//! acknowledgements and whether it added partitions, and each acquire pass
//! needs the piggybacked acknowledgement batches in a shape that no longer
//! borrows the request.

use krabka_protocol::owned::share_fetch_request::{FetchPartition, ShareFetchRequest};

/// One piggybacked acknowledgement batch, that is
/// `(first_offset, last_offset, per-offset acknowledge_types)`.
pub(super) type AckBatch = (i64, i64, Vec<i8>);

/// Whether any request partition carries acknowledgement batches: Kafka's
/// `isAcknowledgeDataPresentInFetchRequest`.
pub(super) fn has_acknowledgements(req: &ShareFetchRequest) -> bool {
    req.topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .any(|partition| !partition.acknowledgement_batches.is_empty())
}

/// Collects the piggybacked acknowledgement batches from a request partition
/// into `(first, last, acknowledge_types)` triples.
pub(super) fn collect_ack_batches(fp: &FetchPartition) -> Vec<AckBatch> {
    fp.acknowledgement_batches
        .iter()
        .map(|b| (b.first_offset, b.last_offset, b.acknowledge_types.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::share_fetch_request::{AcknowledgementBatch, FetchTopic};

    use super::*;

    #[test]
    fn collect_ack_batches_preserves_offsets_and_ack_types() {
        let partition = FetchPartition {
            partition_index: 6,
            acknowledgement_batches: vec![
                AcknowledgementBatch {
                    first_offset: 10,
                    last_offset: 12,
                    acknowledge_types: vec![0, 1, 1],
                    ..Default::default()
                },
                AcknowledgementBatch {
                    first_offset: 30,
                    last_offset: 30,
                    acknowledge_types: Vec::new(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let batches = collect_ack_batches(&partition);

        assert!(batches == vec![(10, 12, vec![0, 1, 1]), (30, 30, Vec::new())]);
    }

    #[test]
    fn acknowledgements_are_present_when_any_partition_carries_a_batch() {
        let request = |partitions| ShareFetchRequest {
            topics: vec![FetchTopic {
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        };
        let addition = FetchPartition {
            partition_index: 1,
            ..Default::default()
        };
        let acknowledgement = FetchPartition {
            partition_index: 2,
            acknowledgement_batches: vec![AcknowledgementBatch::default()],
            ..Default::default()
        };

        assert!(
            [
                has_acknowledgements(&ShareFetchRequest::default()),
                has_acknowledgements(&request(vec![addition.clone()])),
                has_acknowledgements(&request(vec![acknowledgement.clone()])),
                has_acknowledgements(&request(vec![addition, acknowledgement])),
            ] == [false, false, true, true]
        );
    }
}
