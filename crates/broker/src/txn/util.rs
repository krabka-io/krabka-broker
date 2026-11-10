//! Shared utilities for the transaction subsystem.

/// A producer's id and the epoch it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub(crate) producer_id: ProducerId,
    pub(crate) producer_epoch: i16,
}

/// Request version at and above which `AddPartitionsToTxn`, `AddOffsetsToTxn`
/// and `EndTxn` carry `PRODUCER_FENCED` (90, KIP-360). Below it, Kafka's
/// `KafkaApis` downgrades that answer to the legacy `INVALID_PRODUCER_EPOCH`
/// (47).
pub const PRODUCER_FENCED_MIN_VERSION: i16 = 2;

/// Kafka `KafkaApis.handleAddOffsetsToTxnRequest` and `handleEndTxnRequest`:
/// a client below version 2 does not know `PRODUCER_FENCED`, so it gets
/// `INVALID_PRODUCER_EPOCH`. Every other code passes through.
pub fn producer_fenced_wire_code(version: i16, code: i16) -> i16 {
    if version < PRODUCER_FENCED_MIN_VERSION && code == PRODUCER_FENCED {
        INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

use {
    crate::codes::{INVALID_PRODUCER_EPOCH, PRODUCER_FENCED},
    krabka_log::ProducerId,
};

/// Independent expected wire codes for the `EndTxn` response tests, which
/// drive [`producer_fenced_wire_code`] through the error response.
#[cfg(test)]
pub fn producer_fenced_cases() -> [(i16, i16, i16); 5] {
    use crate::codes::{CONCURRENT_TRANSACTIONS, INVALID_PRODUCER_EPOCH, PRODUCER_FENCED};

    [
        (0, PRODUCER_FENCED, INVALID_PRODUCER_EPOCH),
        (1, PRODUCER_FENCED, INVALID_PRODUCER_EPOCH),
        (2, PRODUCER_FENCED, PRODUCER_FENCED),
        (5, PRODUCER_FENCED, PRODUCER_FENCED),
        (0, CONCURRENT_TRANSACTIONS, CONCURRENT_TRANSACTIONS),
    ]
}
