//! Shared utilities for the transaction subsystem.

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Transaction handlers use this to stamp `last_update_ms` on `TxnEntry`.
#[inline]
pub(crate) fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// Request version at and above which `AddPartitionsToTxn`, `AddOffsetsToTxn`
/// and `EndTxn` carry `PRODUCER_FENCED` (90, KIP-360). Below it, Kafka's
/// `KafkaApis` downgrades that answer to the legacy `INVALID_PRODUCER_EPOCH`
/// (47).
pub(crate) const PRODUCER_FENCED_MIN_VERSION: i16 = 2;

/// Kafka `KafkaApis.handleAddOffsetsToTxnRequest` and `handleEndTxnRequest`:
/// a client below version 2 does not know `PRODUCER_FENCED`, so it gets
/// `INVALID_PRODUCER_EPOCH`. Every other code passes through.
pub(crate) fn producer_fenced_wire_code(version: i16, code: i16) -> i16 {
    if version < PRODUCER_FENCED_MIN_VERSION && code == crate::codes::PRODUCER_FENCED {
        crate::codes::INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::codes;

    #[test]
    fn producer_fenced_is_invalid_producer_epoch_below_version_2() {
        // (version, code, expected code on the wire)
        let cases = [
            (0, codes::PRODUCER_FENCED, codes::INVALID_PRODUCER_EPOCH),
            (1, codes::PRODUCER_FENCED, codes::INVALID_PRODUCER_EPOCH),
            (2, codes::PRODUCER_FENCED, codes::PRODUCER_FENCED),
            (5, codes::PRODUCER_FENCED, codes::PRODUCER_FENCED),
            (
                1,
                codes::CONCURRENT_TRANSACTIONS,
                codes::CONCURRENT_TRANSACTIONS,
            ),
        ];
        for (version, code, expected) in cases {
            check!(
                producer_fenced_wire_code(version, code) == expected,
                "v{version} {code}"
            );
        }
    }
}
