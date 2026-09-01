//! Assembly and encoding of the `CreatePartitions` response, including the
//! KIP-599 throttle that the handler applies after it has built the per-topic
//! result rows.

use bytes::Bytes;
use krabka_protocol::{
    Encode,
    api_key::ApiKey,
    owned::create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
};
use krabka_units::{Time, convert::TimeExt};

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

pub(super) async fn finish_response(
    broker: &crate::broker::Broker,
    delay: Time,
    results: Vec<CreatePartitionsTopicResult>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    // The KIP-599 delay is the only throttle this api applies — the dispatch
    // loop marks it quota-exempt and never charges it the request quota — so
    // resolving it through the metric records the throttle phase and the quota
    // that caused it exactly once per request.
    let delay = broker.metrics.record_applied_throttle(
        ApiKey::CreatePartitions as i16,
        &[(crate::metrics::QuotaType::ControllerMutation, delay)],
    );
    let resp = create_partitions_response(results, crate::quota::throttle_time_ms(delay));
    if delay > <Time as TimeExt>::ZERO {
        tokio::time::sleep(delay.to_std()).await;
    }
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
