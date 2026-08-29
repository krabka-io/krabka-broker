//! The minimal `ApiVersions` response that the controller listener returns
//! before a peer authenticates.
//!
//! A JVM client sends `ApiVersions` as its first frame, so the listener must
//! answer it while the connection is still anonymous. The response advertises
//! only the three APIs that are legal before authentication, each with the
//! version range of its generated schema.

use krabka_protocol::owned::{
    api_versions_request,
    api_versions_response::{ApiVersion, ApiVersionsResponse},
    sasl_authenticate_request, sasl_handshake_request,
};

use super::{API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE};

/// Builds the minimal `ApiVersionsResponse` used before SASL authentication.
///
/// Only the three APIs allowed during authentication are advertised. Each
/// entry uses its generated schema range; the ranges are not interchangeable.
/// The generated encoder also handles the v0-v4 body differences, including
/// compact arrays and tagged fields from v3.
pub(super) fn pre_auth_api_versions_response() -> ApiVersionsResponse {
    ApiVersionsResponse {
        api_keys: vec![
            ApiVersion {
                api_key: API_KEY_SASL_HANDSHAKE,
                min_version: sasl_handshake_request::MIN_VERSION,
                max_version: sasl_handshake_request::MAX_VERSION,
                ..Default::default()
            },
            ApiVersion {
                api_key: API_KEY_SASL_AUTHENTICATE,
                min_version: sasl_authenticate_request::MIN_VERSION,
                max_version: sasl_authenticate_request::MAX_VERSION,
                ..Default::default()
            },
            ApiVersion {
                api_key: API_KEY_API_VERSIONS,
                min_version: api_versions_request::MIN_VERSION,
                max_version: api_versions_request::MAX_VERSION,
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::Decode;

    use super::*;
    use crate::raft_handshake::{frame::write_response, test_support::read_response_frame};

    #[tokio::test]
    async fn api_versions_response_uses_schema_ranges_and_versioned_encoding() {
        let expected_ranges = [(17, 0, 1), (36, 0, 2), (18, 0, 5)];

        for version in [0, 3] {
            let (mut client, mut server) = tokio::io::duplex(256);
            let writer = tokio::spawn(async move {
                let response = pre_auth_api_versions_response();
                write_response(&mut server, API_KEY_API_VERSIONS, version, 99, &response)
                    .await
                    .expect("write api versions response");
            });
            let frame = read_response_frame(&mut client).await;
            writer.await.expect("writer");

            assert!(&frame[..4] == &99i32.to_be_bytes());
            let mut body = &frame[4..];
            let response = ApiVersionsResponse::decode(&mut body, version)
                .expect("decode api versions response");
            assert!(body.is_empty());
            let ranges: Vec<_> = response
                .api_keys
                .iter()
                .map(|api| (api.api_key, api.min_version, api.max_version))
                .collect();
            assert!(ranges == expected_ranges);

            if version == 0 {
                // v0 has no throttle_time_ms field. The old hand-rolled
                // response appended one and produced a malformed frame.
                assert!(frame.len() == 28);
            }
        }
    }
}
