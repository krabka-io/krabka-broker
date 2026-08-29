//! The `EndTxnResponse` encoders. One shape carries the successful completion
//! identity, the other the wire sentinels that stand in for it when the handler
//! answers with an error code.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{Encode, owned::end_txn_response::EndTxnResponse};

use crate::{codes, error::BrokerError};

/// Kafka wire sentinel: "no producer id" (`RecordBatch.NO_PRODUCER_ID`).
/// Returned on `EndTxn` error responses, where the identity is meaningless.
const NO_PRODUCER_ID: i64 = -1;

/// Kafka wire sentinel: "no producer epoch" (`RecordBatch.NO_PRODUCER_EPOCH`).
const NO_PRODUCER_EPOCH: i16 = -1;

pub(super) fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    // On the error path the producer_id/epoch fields are not meaningful;
    // leave them at the "no producer" wire sentinels.
    encode_response(version, error_code, NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
}

/// Encode a successful `EndTxn` response. `producer_id` and `producer_epoch`
/// are the post-completion identity. The epoch bumps at `TV >= 2`, or rolls to a
/// new `producer_id` on epoch exhaustion; see
/// [`next_producer_identity`](super::producer_identity::next_producer_identity). They
/// are only on the wire at v5 (KIP-890). At lower versions the producer never
/// observes them, and the persisted bump instead fences a stale-epoch producer
/// on its next coordinator call.
pub(super) fn encode_ok(
    version: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<Bytes, BrokerError> {
    encode_response(version, codes::NONE, producer_id, producer_epoch)
}

fn encode_response(
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<Bytes, BrokerError> {
    let resp = EndTxnResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id,
        producer_epoch,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;

    fn decode_response(bytes: &Bytes, version: i16) -> EndTxnResponse {
        crate::test_support::decode_response(bytes, version)
    }

    #[test]
    fn encode_err_leaves_producer_identity_at_error_sentinels() {
        let bytes = encode_err(5, codes::NOT_COORDINATOR).expect("encode error");
        assert!(!bytes.is_empty());
        let resp = decode_response(&bytes, 5);

        let expected = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NOT_COORDINATOR,
            producer_id: -1,
            producer_epoch: -1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn encode_ok_returns_v5_producer_identity() {
        let bytes = encode_ok(5, 42, 7).expect("encode ok");
        assert!(!bytes.is_empty());
        let resp = decode_response(&bytes, 5);

        let expected = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            producer_id: 42,
            producer_epoch: 7,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }
}
