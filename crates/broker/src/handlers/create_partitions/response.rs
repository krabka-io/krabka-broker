//! Assembly and encoding of the `CreatePartitions` response, including the
//! KIP-599 throttle that the handler records after it has built the per-topic
//! result rows.

use bytes::Bytes;
use krabka_protocol::{
    Encode,
    owned::create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
};
use krabka_units::Time;

use crate::error::BrokerError;

pub(super) fn create_partitions_response(
    results: Vec<CreatePartitionsTopicResult>,
    throttle_time_ms: i32,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results,
        throttle_time_ms,
        ..Default::default()
    }
}

pub(super) fn encode_response<R: Encode>(resp: &R, version: i16) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

pub(super) fn finish_response(
    context: &crate::handlers::RequestContext<'_>,
    delay: Time,
    results: Vec<CreatePartitionsTopicResult>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    let resp = create_partitions_response(results, crate::quota::throttle_time_ms(delay));
    // KIP-219: the KIP-599 window is reported here and enforced by the
    // connection loop, which mutes the connection after the response is sent.
    context.record_throttle(delay);
    encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{codes, handlers::create_partitions::test_support::VERSION};

    fn decode_response(bytes: &Bytes) -> CreatePartitionsResponse {
        crate::test_support::decode_response(bytes, VERSION)
    }

    #[test]
    fn encode_response_writes_decodable_results_and_throttle() {
        let bytes = encode_response(
            &create_partitions_response(
                vec![CreatePartitionsTopicResult {
                    name: "orders".into(),
                    error_code: codes::INVALID_PARTITIONS,
                    error_message: Some("bad count".into()),
                    ..Default::default()
                }],
                321,
            ),
            VERSION,
        )
        .expect("encode");
        let resp = decode_response(&bytes);

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 321,
            results: vec![CreatePartitionsTopicResult {
                name: "orders".into(),
                error_code: codes::INVALID_PARTITIONS,
                error_message: Some("bad count".into()),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }
}
