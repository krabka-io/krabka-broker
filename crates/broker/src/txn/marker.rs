//! Control-record construction. A commit/abort marker is a single-
//! record `RecordBatch` with `is_control_batch=true` and
//! `is_transactional=true` in attributes.
//!
//! The record key layout matches Apache Kafka `EndTransactionMarker`:
//!   version: i16 (big-endian) = 0
//!   type:    i16 (big-endian), 0 = ABORT, 1 = COMMIT
//! The record value matches Kafka's `EndTxnMarker` schema:
//!   version:           i16 (big-endian) = 0
//!   `coordinator_epoch`: i32 (big-endian)

use bytes::Bytes;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use crate::{codes, error::BrokerError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerType {
    Commit,
    Abort,
}

impl MarkerType {
    fn type_code(self) -> i16 {
        match self {
            MarkerType::Commit => 1,
            MarkerType::Abort => 0,
        }
    }
}

/// How the `EndTxn` marker fan-out should react to one partition's marker
/// write failing.
///
/// Kafka's `TransactionMarkerRequestCompletionHandler` classifies every
/// per-partition `WriteTxnMarkers` code this way: a retriable code goes back
/// into the retry queue for that partition alone, and a fencing code cancels
/// the whole fan-out for this producer generation (a newer generation has
/// already superseded it, so retrying cannot succeed and must not corrupt
/// state). Every other outcome -- including a code this broker does not
/// otherwise recognize -- is treated as retriable, since retrying is always
/// safe (`append_marker_and_materialize` is idempotent per generation) while
/// giving up early is not (#882).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerFailureClass {
    /// Retry this partition; the fan-out has not been superseded.
    Retriable,
    /// A newer producer or coordinator generation has fenced this fan-out.
    /// Retrying cannot succeed, so the fan-out must stop.
    Fatal,
}

/// Classify one marker-write failure by the wire code it maps to.
///
/// Both the local append path
/// ([`append_marker_and_materialize`](crate::txn::handlers::write_txn_markers::append_marker_and_materialize),
/// via [`BrokerError::ProducerEpochFenced`] / [`BrokerError::CoordinatorEpochFenced`])
/// and the remote `WriteTxnMarkers` RPC (via [`BrokerError::MarkerWriteRefused`])
/// go through this one classification, so a fenced producer or coordinator
/// generation cancels the fan-out the same way whether the failing partition
/// is local or remote.
pub(crate) fn classify_marker_failure(error: &BrokerError) -> MarkerFailureClass {
    let code = match error {
        BrokerError::ProducerEpochFenced { .. } => codes::INVALID_PRODUCER_EPOCH,
        BrokerError::CoordinatorEpochFenced { .. } => codes::TRANSACTION_COORDINATOR_FENCED,
        BrokerError::MarkerWriteRefused { code, .. } => *code,
        _ => return MarkerFailureClass::Retriable,
    };
    match code {
        codes::INVALID_PRODUCER_EPOCH | codes::TRANSACTION_COORDINATOR_FENCED => {
            MarkerFailureClass::Fatal
        }
        _ => MarkerFailureClass::Retriable,
    }
}

pub fn build_marker_batch(
    producer_id: ProducerId,
    producer_epoch: i16,
    base_offset: Offset,
    marker_type: MarkerType,
    coordinator_epoch: i32,
) -> RecordBatch {
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes()); // version
    key.extend_from_slice(&marker_type.type_code().to_be_bytes());

    let mut value = Vec::with_capacity(6);
    value.extend_from_slice(&0i16.to_be_bytes()); // version
    value.extend_from_slice(&coordinator_epoch.to_be_bytes());

    let attrs = Attributes::default()
        .with_transactional(true)
        .with_control(true);

    RecordBatch {
        attributes: attrs,
        base_offset: base_offset.0,
        last_offset_delta: 0,
        // Unwrap into the raw-`i64` protocol `RecordBatch` field at the wire seam.
        producer_id: producer_id.get(),
        producer_epoch,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn classifies_fenced_generations_as_fatal_and_everything_else_as_retriable() {
        let cases: &[(BrokerError, MarkerFailureClass)] = &[
            (
                BrokerError::ProducerEpochFenced {
                    producer_id: 7,
                    current: 3,
                    requested: 1,
                },
                MarkerFailureClass::Fatal,
            ),
            (
                BrokerError::CoordinatorEpochFenced {
                    current: 9,
                    requested: 3,
                },
                MarkerFailureClass::Fatal,
            ),
            (
                BrokerError::MarkerWriteRefused {
                    code: codes::INVALID_PRODUCER_EPOCH,
                    message: "fenced".into(),
                },
                MarkerFailureClass::Fatal,
            ),
            (
                BrokerError::MarkerWriteRefused {
                    code: codes::TRANSACTION_COORDINATOR_FENCED,
                    message: "fenced".into(),
                },
                MarkerFailureClass::Fatal,
            ),
            (
                BrokerError::MarkerWriteRefused {
                    code: codes::NOT_LEADER_OR_FOLLOWER,
                    message: "retry".into(),
                },
                MarkerFailureClass::Retriable,
            ),
            (
                BrokerError::MarkerWriteRefused {
                    code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message: "retry".into(),
                },
                MarkerFailureClass::Retriable,
            ),
            (
                BrokerError::MarkerWriteRefused {
                    code: codes::REQUEST_TIMED_OUT,
                    message: "retry".into(),
                },
                MarkerFailureClass::Retriable,
            ),
            (
                BrokerError::Txn("connect failed".into()),
                MarkerFailureClass::Retriable,
            ),
        ];
        for (error, expected) in cases {
            assert!(classify_marker_failure(error) == *expected, "{error:?}");
        }
    }

    #[test]
    fn commit_marker_attribute_bits_set() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(7), MarkerType::Commit, 19);
        assert!(b.attributes.is_transactional());
        assert!(b.attributes.is_control_batch());
    }

    #[test]
    fn abort_marker_key_starts_with_version_zero_then_type_zero() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Abort, 19);
        let key = b.records[0].key.as_ref().unwrap();
        // i16 BE version 0, then i16 BE control type 0 (abort).
        assert!(&key[..] == &[0u8, 0, 0, 0][..]);
    }

    #[test]
    fn commit_marker_key_type_is_one() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let key = b.records[0].key.as_ref().unwrap();
        assert!(&key[2..] == &1i16.to_be_bytes());
    }

    #[test]
    fn marker_value_contains_version_and_coordinator_epoch() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let value = b.records[0].value.as_ref().unwrap();
        assert!(&value[..2] == &0i16.to_be_bytes());
        assert!(&value[2..] == &19i32.to_be_bytes());
    }
}
