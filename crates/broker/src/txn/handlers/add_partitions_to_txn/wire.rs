//! Encoding of the `AddPartitionsToTxnResponse`.
//!
//! The response carries two mutually exclusive result arrays — v0-3 fills
//! `results_by_topic_v3_and_below` and v4-5 fills `results_by_transaction` —
//! so the encoder is shared by both version paths and the array each one
//! leaves empty is what selects the wire shape.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{Encode, owned::add_partitions_to_txn_response::AddPartitionsToTxnResponse};

use crate::error::BrokerError;

pub(super) fn encode_response(
    resp: &AddPartitionsToTxnResponse,
    version: i16,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::add_partitions_to_txn_response::AddPartitionsToTxnResult;

    use super::*;
    use crate::{
        codes,
        txn::handlers::add_partitions_to_txn::{
            results::topic_error,
            test_support::{topic, topic_result},
        },
    };

    fn decode_response(bytes: &Bytes, version: i16) -> AddPartitionsToTxnResponse {
        crate::test_support::decode_response(bytes, version)
    }

    #[test]
    fn encode_response_round_trips_v4_transaction_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: topic_error(&[topic("alpha", &[1])], codes::INVALID_TXN_STATE),
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = encode_response(&resp, 4).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 4);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: vec![topic_result("alpha", &[(1, codes::INVALID_TXN_STATE)])],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }

    #[test]
    fn encode_response_round_trips_v3_topic_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: topic_error(&[topic("alpha", &[7])], codes::NONE),
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 3);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result("alpha", &[(7, codes::NONE)])],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }
}
