//! Kafka request and response framing for the controller-listener handshake,
//! the server-side inverse of `network::client::round_trip`.
//!
//! The module reads one length-prefixed request frame at a time, strips the
//! `RequestHeader`, and writes the matching length-prefixed response frame
//! with the `ResponseHeader` that the peer expects. The header-flexibility
//! predicates that decide between the v1 and v2 request headers and the v0
//! and v1 response headers live here as well, because they are what makes
//! the two directions agree.

use {
    super::{API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, SASL_AUTHENTICATE_FLEXIBLE_VERSION},
    crate::network::codec::validate_frame_length,
    krabka_client_core::ClientDuplex,
    krabka_protocol::{
        Decode, Encode,
        owned::{api_versions_request, request_header::RequestHeader},
    },
    krabka_raft::RaftHandshakeError,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

/// The api key, version and correlation id that a request names, which its
/// response repeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RequestIds {
    pub(super) api_key: i16,
    pub(super) api_version: i16,
    pub(super) correlation_id: i32,
}

/// A request frame with its `RequestHeader` removed.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct KafkaRequest {
    pub(super) ids: RequestIds,
    pub(super) body: Vec<u8>,
}

/// Reads one length-prefixed Kafka request frame, removes the
/// `RequestHeader` (v1 or v2), and returns the request's ids and body bytes.
///
/// The header parsing matches the outbound encoder in
/// `network::client::round_trip`:
/// - v1, non-flexible: `api_key i16 | api_version i16 | corr_id i32 |
///   client_id i16-length-prefixed bytes`.
/// - v2, flexible, which `SaslAuthenticate v2+` and `ApiVersions v3+` use:
///   the v1 layout plus a tagged-fields section.
///
/// A size prefix over `max_frame_bytes` fails before the frame is read.
/// Kafka's `SaslServerAuthenticator` turns the `InvalidReceiveException` of
/// that read into a `SaslAuthenticationException`, so the failure is a SASL
/// one.
pub(super) async fn read_kafka_request(
    stream: &mut dyn ClientDuplex,
    max_frame_bytes: usize,
) -> Result<KafkaRequest, RaftHandshakeError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;
    validate_frame_length(size, max_frame_bytes).map_err(|_| {
        RaftHandshakeError::Sasl("Failing SASL authentication due to invalid receive size".into())
    })?;
    let mut frame = vec![0u8; size];
    stream.read_exact(&mut frame).await?;
    let [api_key_hi, api_key_lo, api_version_hi, api_version_lo, ..] = frame.as_slice() else {
        return Err(RaftHandshakeError::Protocol("short request header".into()));
    };
    let api_key = i16::from_be_bytes([*api_key_hi, *api_key_lo]);
    let api_version = i16::from_be_bytes([*api_version_hi, *api_version_lo]);
    let header_version = if is_request_header_flexible(api_key, api_version) {
        2
    } else {
        1
    };
    let mut body = frame.as_slice();
    let header = RequestHeader::decode(&mut body, header_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    Ok(KafkaRequest {
        ids: RequestIds {
            api_key: header.request_api_key,
            api_version: header.request_api_version,
            correlation_id: header.correlation_id,
        },
        body: body.to_vec(),
    })
}

/// Encodes `resp`, prepends the `ResponseHeader` (v0 or v1 by the rules
/// below), and writes the length-prefixed frame.
pub(super) async fn write_response<R: Encode>(
    stream: &mut dyn ClientDuplex,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    resp: &R,
) -> Result<(), RaftHandshakeError> {
    let mut body = Vec::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut body, api_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    write_response_body(stream, api_key, api_version, corr_id, &body).await
}

/// Prepends the `ResponseHeader` (v0 or v1 by the rules below) to an encoded
/// response body, and writes the length-prefixed frame.
pub(super) async fn write_response_body(
    stream: &mut dyn ClientDuplex,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    body: &[u8],
) -> Result<(), RaftHandshakeError> {
    let flexible = is_response_header_flexible(api_key, api_version);
    let header_len = 4 + usize::from(flexible);
    let total = header_len + body.len();
    let total_u32 = u32::try_from(total)
        .map_err(|_| RaftHandshakeError::Protocol("response frame exceeds u32".into()))?;

    let mut out = Vec::with_capacity(4 + total);
    out.extend_from_slice(&total_u32.to_be_bytes());
    out.extend_from_slice(&corr_id.to_be_bytes());
    if flexible {
        out.push(0); // empty tagged-fields
    }
    out.extend_from_slice(body);
    stream.write_all(&out).await?;
    Ok(())
}

/// Request-header flexibility rules.
///
/// Mirrors the generated protocol schema. `SaslAuthenticate` becomes flexible
/// at v2 and `ApiVersions` at v3. `SaslHandshake` v0-v1 stays non-flexible.
fn is_request_header_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        API_KEY_API_VERSIONS => api_version >= api_versions_request::FLEXIBLE_MIN,
        _ => false,
    }
}

/// Response-header flexibility rules.
///
/// - `SaslHandshake (17)`: non-flexible at every version this module accepts.
/// - `SaslAuthenticate (36)`: flexible from v2.
/// - `ApiVersions (18)`: *always* a v0 response header by Kafka spec,
///   whatever the body flexibility. The Kafka clients special-case it.
fn is_response_header_flexible(api_key: i16, api_version: i16) -> bool {
    // SaslHandshake (17) and ApiVersions (18) keep the v0 response header
    // at every version we accept; only SaslAuthenticate (36) flips to a
    // flexible response header starting at v2.
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::raft_handshake::{
            API_KEY_SASL_HANDSHAKE,
            test_support::{
                HandshakeApiKey, HandshakeApiVersion, HandshakeHeader, read_request_from_frame,
                read_response_frame,
            },
        },
        assert2::assert,
        bytes::BufMut,
        std::mem,
        tokio::io,
    };

    fn request(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> KafkaRequest {
        KafkaRequest {
            ids: RequestIds {
                api_key,
                api_version,
                correlation_id,
            },
            body: body.to_vec(),
        }
    }

    struct FixedResp(&'static [u8]);

    impl Encode for FixedResp {
        fn encode<B: BufMut>(
            &self,
            buf: &mut B,
            _version: i16,
        ) -> Result<(), krabka_protocol::ProtocolError> {
            buf.put_slice(self.0);
            Ok(())
        }

        fn encoded_len(&self, _version: i16) -> usize { self.0.len() }
    }

    /// A request's api key and version, and the header it must be read and
    /// answered with.
    struct HeaderCase {
        api_key: HandshakeApiKey,
        version: HandshakeApiVersion,
        want: HandshakeHeader,
    }

    const SASL_HEADER_CASES: [HeaderCase; 4] = [
        HeaderCase {
            api_key: HandshakeApiKey(API_KEY_SASL_HANDSHAKE),
            version: HandshakeApiVersion(0),
            want: HandshakeHeader::Legacy,
        },
        HeaderCase {
            api_key: HandshakeApiKey(API_KEY_SASL_HANDSHAKE),
            version: HandshakeApiVersion(1),
            want: HandshakeHeader::Legacy,
        },
        HeaderCase {
            api_key: HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
            version: HandshakeApiVersion(1),
            want: HandshakeHeader::Legacy,
        },
        HeaderCase {
            api_key: HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
            version: HandshakeApiVersion(2),
            want: HandshakeHeader::Flexible,
        },
    ];

    #[test]
    fn header_flexibility_table_matches_outbound_encoder() {
        // SASL request and response headers share the version rules above.
        for HeaderCase {
            api_key,
            version,
            want,
        } in SASL_HEADER_CASES.into_iter().chain([
            HeaderCase {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                version: HandshakeApiVersion(2),
                want: HandshakeHeader::Legacy,
            },
            HeaderCase {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                version: HandshakeApiVersion(3),
                want: HandshakeHeader::Flexible,
            },
        ]) {
            assert!(
                is_request_header_flexible(api_key.0, version.0) == want.is_flexible(),
                "request api_key {} v{}",
                api_key.0,
                version.0
            );
        }
        // ApiVersions responses always use header v0, including request v3.
        for HeaderCase {
            api_key,
            version,
            want,
        } in SASL_HEADER_CASES.into_iter().chain([
            HeaderCase {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                version: HandshakeApiVersion(0),
                want: HandshakeHeader::Legacy,
            },
            HeaderCase {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                version: HandshakeApiVersion(3),
                want: HandshakeHeader::Legacy,
            },
        ]) {
            assert!(
                is_response_header_flexible(api_key.0, version.0) == want.is_flexible(),
                "response api_key {} v{}",
                api_key.0,
                version.0
            );
        }
    }

    #[derive(Default)]
    struct HandshakeRequestFrame {
        frame: Vec<u8>,
        stream: Option<io::DuplexStream>,
        decoded: Option<Result<KafkaRequest, RaftHandshakeError>>,
    }

    #[rstest::fixture]
    fn handshake_request_frame() -> HandshakeRequestFrame {
        // Each peer presents fresh bytes and observes one request read.
        HandshakeRequestFrame::default()
    }

    #[rstest_bdd_macros::given("the controller peer sent Kafka frame hex {frame}")]
    fn supplied_handshake_frame(
        handshake_request_frame: &mut HandshakeRequestFrame,
        frame: String,
    ) {
        handshake_request_frame.frame =
            hex::decode(frame).expect("explicit complete Kafka frame bytes");
    }

    #[rstest_bdd_macros::given(
        "the controller peer sent size prefix {size} and closed without a payload"
    )]
    async fn supplied_handshake_size(
        handshake_request_frame: &mut HandshakeRequestFrame,
        size: u32,
    ) {
        let (mut client, server) = io::duplex(64);
        client
            .write_all(&size.to_be_bytes())
            .await
            .expect("write size prefix");
        drop(client);
        handshake_request_frame.stream = Some(server);
    }

    #[rstest_bdd_macros::when("the controller handshake reads the Kafka request frame")]
    async fn read_supplied_handshake_frame(handshake_request_frame: &mut HandshakeRequestFrame) {
        handshake_request_frame.decoded =
            Some(read_request_from_frame(mem::take(&mut handshake_request_frame.frame)).await);
    }

    #[rstest_bdd_macros::when(
        "the controller handshake admits a frame with SASL receive limit 4096"
    )]
    async fn admit_supplied_handshake_frame(handshake_request_frame: &mut HandshakeRequestFrame) {
        handshake_request_frame.decoded = Some(
            read_kafka_request(
                handshake_request_frame
                    .stream
                    .as_mut()
                    .expect("peer stream was given"),
                4096,
            )
            .await,
        );
    }

    #[rstest_bdd_macros::then(
        "the request has API {key} version {version}, correlation {correlation}, and body {body}"
    )]
    fn decoded_handshake_frame(
        handshake_request_frame: &HandshakeRequestFrame,
        key: i16,
        version: i16,
        correlation: i32,
        body: String,
    ) {
        let body = if body == "empty" {
            b"".as_slice()
        } else {
            body.as_bytes()
        };
        let actual = handshake_request_frame
            .decoded
            .as_ref()
            .expect("request frame was read")
            .as_ref()
            .expect("valid request frame");
        assert!(*actual == request(key, version, correlation, body));
    }

    #[rstest_bdd_macros::then(
        "the controller handshake rejects the malformed request as a protocol error"
    )]
    fn refused_handshake_frame(handshake_request_frame: &HandshakeRequestFrame) {
        assert!(matches!(
            handshake_request_frame
                .decoded
                .as_ref()
                .expect("request frame was read"),
            Err(RaftHandshakeError::Protocol(_))
        ));
    }

    #[rstest_bdd_macros::then("frame admission reaches {outcome}")]
    fn handshake_size_admission(handshake_request_frame: &HandshakeRequestFrame, outcome: String) {
        let actual = handshake_request_frame
            .decoded
            .as_ref()
            .expect("size prefix was admitted");
        match outcome.as_str() {
            "SASL receive-size failure" => {
                assert!(matches!(actual, Err(RaftHandshakeError::Sasl(_))));
            }
            "payload reading after an admitted prefix" => {
                assert!(!matches!(actual, Err(RaftHandshakeError::Sasl(_))));
            }
            other => panic!("unknown expected admission outcome {other}"),
        }
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/raft_handshake/frame/valid_requests.feature",
        name = "Read a valid Kafka header and payload"
    )]
    #[tokio::test]
    async fn decoded_controller_request(
        handshake_request_frame: HandshakeRequestFrame,
        frame: String,
        key: i16,
        version: i16,
        correlation: i32,
        body: String,
    ) {
        let _ = (
            handshake_request_frame,
            frame,
            key,
            version,
            correlation,
            body,
        );
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/raft_handshake/frame/malformed_requests.feature",
        name = "Refuse a malformed Kafka request header"
    )]
    #[tokio::test]
    async fn refused_controller_request(
        handshake_request_frame: HandshakeRequestFrame,
        case: String,
        frame: String,
    ) {
        let _ = (handshake_request_frame, case, frame);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/raft_handshake/frame/receive_limit.feature",
        name = "Check the inclusive SASL receive-size limit before payload reads"
    )]
    #[tokio::test]
    async fn controller_request_receive_limit(
        handshake_request_frame: HandshakeRequestFrame,
        size: u32,
        outcome: String,
    ) {
        let _ = (handshake_request_frame, size, outcome);
    }

    #[tokio::test]
    async fn write_response_uses_expected_header_versions() {
        let (mut client, mut server) = io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_AUTHENTICATE,
                2,
                77,
                &FixedResp(&[0xaa, 0xbb]),
            )
            .await
            .expect("write flexible");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        // corr_id 77 BE + empty tagged-fields byte (flexible header) + body.
        let expected: Vec<u8> = [77i32.to_be_bytes().as_slice(), &[0x00], &[0xaa, 0xbb]].concat();
        assert!(frame == expected);

        let (mut client, mut server) = io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_HANDSHAKE,
                1,
                78,
                &FixedResp(&[0xcc]),
            )
            .await
            .expect("write nonflex");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        assert!(&frame[0..4] == &78i32.to_be_bytes());
        assert!(&frame[4..] == &[0xcc]);
    }
}
