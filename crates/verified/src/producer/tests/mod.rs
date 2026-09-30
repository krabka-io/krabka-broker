use super::*;

const ENTRY: ProducerEntryFacts = ProducerEntryFacts {
    epoch: 2,
    last_sequence: 6,
};

/// Kafka's five retained batches for a producer that sent sequences
/// 0..=6 at epoch 2 in batches `[0]`, `[1, 2]`, `[3]`, `[4, 5, 6]`, with
/// one slot unused.
const RETAINED: [Option<RetainedSequenceRange>; 5] = [
    None,
    Some(RetainedSequenceRange {
        base_sequence: 0,
        last_sequence: 0,
    }),
    Some(RetainedSequenceRange {
        base_sequence: 1,
        last_sequence: 2,
    }),
    Some(RetainedSequenceRange {
        base_sequence: 3,
        last_sequence: 3,
    }),
    Some(RetainedSequenceRange {
        base_sequence: 4,
        last_sequence: 6,
    }),
];

mod sequence_arithmetic_matches_kafka_wraparound;
