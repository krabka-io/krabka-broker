//! Tests for the response-framing benchmark seam.
//!
//! `benches/perf_deferrals.rs` prices a chained-`Buf` prototype against the
//! path the dispatch loop actually takes, and that comparison only means
//! anything while the two put the *same* bytes on the wire. The prototype is
//! assembled out of [`response_header_len`] and [`response_header_v1`]; the
//! path it is priced against is [`encode_response`] followed by the [`codec`]
//! the connection loop frames its stream with. Four functions, one wire
//! format, and nothing in a `cargo test` run had been holding them together.
//!
//! So these tests pin all four against the Kafka response header restated
//! here, and against the bytes a real `Framed` sink writes. A header shape
//! that drifts now fails a test, instead of quietly re-weighing a PERF note
//! whose "keep" the benchmark settled.

use assert2::assert;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::SinkExt as _;
use krabka_protocol::api_key::ApiKey;
use tokio::io::AsyncReadExt as _;
use tokio_util::codec::{Encoder as _, Framed, LengthDelimitedCodec};

use super::{codec, encode_response, response_header_len, response_header_v1};
use crate::handlers::{ApiKeyCode, CorrelationId};

/// Kafka's default `socket.request.max.bytes`, which is what the dispatch loop
/// validates a framed response against.
const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// The correlation id every case echoes. Distinctive in all four bytes, so a
/// truncated or byte-swapped write cannot pass.
const CORRELATION_ID: CorrelationId = 0x0102_0304;

const API_VERSIONS: ApiKeyCode = ApiKey::ApiVersions as i16;
const METADATA: ApiKeyCode = ApiKey::Metadata as i16;

/// One framing case: the api key, the flexibility of the body the handler
/// produced, and how many bytes that body is.
struct Case {
    name: &'static str,
    api_key: ApiKeyCode,
    body_flexible: bool,
    body_len: usize,
}

/// Both header shapes, on both sides of the `ApiVersions` exception, plus the
/// empty body that has nothing but a header to carry.
const CASES: [Case; 5] = [
    Case {
        name: "a flexible body takes the v1 header",
        api_key: METADATA,
        body_flexible: true,
        body_len: 1024,
    },
    Case {
        name: "a non-flexible body takes the v0 header",
        api_key: METADATA,
        body_flexible: false,
        body_len: 1024,
    },
    Case {
        name: "ApiVersions keeps the v0 header even when its body is flexible",
        api_key: API_VERSIONS,
        body_flexible: true,
        body_len: 37,
    },
    Case {
        name: "a non-flexible ApiVersions body takes the same v0 header",
        api_key: API_VERSIONS,
        body_flexible: false,
        body_len: 37,
    },
    Case {
        name: "an empty flexible body still carries its tagged-fields byte",
        api_key: METADATA,
        body_flexible: true,
        body_len: 0,
    },
];

/// A response body of `len` bytes, in a non-uniform pattern so nothing
/// downstream can shortcut it and a truncation cannot land on a repeat.
fn body(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|b| u8::try_from(b % 251).expect("b % 251 fits in a byte"))
            .collect::<Vec<u8>>(),
    )
}

/// The response header Kafka puts in front of a body, restated here rather
/// than read back out of the broker: the correlation id, followed by an empty
/// tagged-fields byte when the body is flexible. `ApiVersions` is the standing
/// exception and stays on the v0 header at every version, because a client has
/// to parse that response before it knows which versions the broker speaks.
fn expected_header(api_key: ApiKeyCode, body_flexible: bool) -> Bytes {
    let mut header = BytesMut::new();
    header.put_i32(CORRELATION_ID);
    if body_flexible && api_key != API_VERSIONS {
        header.put_u8(0);
    }
    header.freeze()
}

/// The bytes a sink framed with `codec` actually writes for `response`.
///
/// A real `Framed`, driven with `send`, which is the pair of `start_send` and
/// `poll_flush` that `serve_connection_stream` drives per response. Dropping
/// it closes the write half so the read side sees EOF.
async fn wire_bytes(response: Bytes, codec: LengthDelimitedCodec) -> Vec<u8> {
    let (client, mut server) = tokio::io::duplex(1024 * 1024);
    let mut framed = Framed::new(client, codec);
    framed
        .send(response)
        .await
        .expect("a duplex write succeeds");
    drop(framed);
    let mut wire = Vec::new();
    server
        .read_to_end(&mut wire)
        .await
        .expect("read the framed response back");
    wire
}

/// The chained-`Buf` prototype's wire image, assembled out of the two header
/// helpers the way `benches/perf_deferrals.rs` assembles it: the codec's
/// 4-byte frame length and the response header in one leading segment, then
/// the handler's body.
fn chained_prototype_wire(api_key: ApiKeyCode, body_flexible: bool, body: &Bytes) -> Vec<u8> {
    let header_len = response_header_len(api_key, body_flexible);
    let mut wire = BytesMut::with_capacity(4 + header_len + body.len());
    wire.put_u32(u32::try_from(header_len + body.len()).expect("a test body fits in a frame"));
    wire.put_i32(CORRELATION_ID);
    if response_header_v1(api_key, body_flexible) {
        wire.put_u8(0); // empty tagged fields
    }
    wire.put_slice(body);
    wire.to_vec()
}

/// The seam's `encode_response` is the dispatch loop's own, and what it
/// produces is the header this test spells out followed by the untouched body.
/// Both header helpers describe exactly that header.
#[test]
fn the_seam_encodes_the_response_header_the_dispatch_loop_encodes() {
    for case in CASES {
        let payload = body(case.body_len);
        let header = expected_header(case.api_key, case.body_flexible);

        assert!(
            response_header_len(case.api_key, case.body_flexible) == header.len(),
            "{}",
            case.name
        );
        assert!(
            response_header_v1(case.api_key, case.body_flexible) == (header.len() == 5),
            "{}",
            case.name
        );

        let framed = encode_response(
            case.api_key,
            CORRELATION_ID,
            case.body_flexible,
            &payload,
            MAX_FRAME_BYTES,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));

        let mut expected = BytesMut::from(&header[..]);
        expected.put_slice(&payload);
        assert!(framed.to_vec() == expected.to_vec(), "{}", case.name);

        // The seam exists only because the production function is
        // crate-internal. A reimplementation that drifted from it shows up
        // here rather than in a benchmark number nobody re-derives.
        let production = super::super::response::encode_response(
            case.api_key,
            CORRELATION_ID,
            case.body_flexible,
            &payload,
            MAX_FRAME_BYTES,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));
        assert!(framed == production, "{}", case.name);
    }
}

/// The seam's codec writes what the connection loop's codec writes, and both
/// write the length-prefixed frame this test spells out.
#[tokio::test]
async fn the_seam_codec_writes_the_frame_the_connection_loop_writes() {
    for case in CASES {
        let payload = body(case.body_len);
        let framed = encode_response(
            case.api_key,
            CORRELATION_ID,
            case.body_flexible,
            &payload,
            MAX_FRAME_BYTES,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));

        let mut expected = BytesMut::new();
        expected.put_u32(u32::try_from(framed.len()).expect("a test frame fits in a u32"));
        expected.put_slice(&framed);
        let expected = expected.to_vec();

        let seam = wire_bytes(framed.clone(), codec(MAX_FRAME_BYTES)).await;
        let production = wire_bytes(framed, crate::network::codec::codec(MAX_FRAME_BYTES)).await;

        assert!(seam == expected, "{}", case.name);
        assert!(production == expected, "{}", case.name);
    }
}

/// The prototype the bench prices against this path is byte-identical to it.
///
/// This is the invariant the whole "saved ns" column rests on: were the header
/// helpers to disagree with `encode_response`, the bench would be timing two
/// different amounts of work and reporting a saving that does not exist. The
/// bench asserts it too, but nothing in CI runs the bench.
#[tokio::test]
async fn the_chained_prototype_the_bench_prices_is_wire_identical() {
    for case in CASES {
        let payload = body(case.body_len);
        let framed = encode_response(
            case.api_key,
            CORRELATION_ID,
            case.body_flexible,
            &payload,
            MAX_FRAME_BYTES,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));

        let copy_path = wire_bytes(framed, codec(MAX_FRAME_BYTES)).await;
        let prototype = chained_prototype_wire(case.api_key, case.body_flexible, &payload);

        assert!(copy_path == prototype, "{}", case.name);
    }
}

/// `encode_response` refuses a frame over the maximum, at the same boundary
/// the dispatch loop's own encoder refuses it. The header counts towards the
/// limit, so the boundary sits `header_len` below the body length.
#[test]
fn the_seam_enforces_the_frame_maximum_at_the_dispatch_boundary() {
    // (body length, max frame bytes, accepted). The api key and flexibility
    // below give a 4-byte header, so a body of `n` needs `n + 4`.
    let cases = [
        (4_usize, 9_usize, true),
        (4, 8, true),
        (4, 7, false),
        (0, 4, true),
        (0, 3, false),
    ];

    for (body_len, max_frame_bytes, accepted) in cases {
        let payload = body(body_len);
        let seam = encode_response(METADATA, CORRELATION_ID, false, &payload, max_frame_bytes);
        let production = super::super::response::encode_response(
            METADATA,
            CORRELATION_ID,
            false,
            &payload,
            max_frame_bytes,
        );

        assert!(
            seam.is_ok() == accepted,
            "{body_len} bytes under {max_frame_bytes}"
        );
        assert!(
            seam.is_ok() == production.is_ok(),
            "{body_len} bytes under {max_frame_bytes}"
        );
    }
}

/// The maximum handed to the seam's codec reaches the codec, so a benchmark
/// sink rejects an oversized frame exactly as a connection would.
#[test]
fn the_seam_codec_carries_the_frame_maximum_into_the_sink() {
    let cases = [(8_usize, true), (9, false)];

    for (frame_len, accepted) in cases {
        let frame = Bytes::from(vec![0_u8; frame_len]);
        let mut seam = BytesMut::new();
        let mut production = BytesMut::new();

        let seam = codec(8).encode(frame.clone(), &mut seam);
        let production = crate::network::codec::codec(8).encode(frame, &mut production);

        assert!(
            seam.is_ok() == accepted,
            "a {frame_len}-byte frame under a max of 8"
        );
        assert!(
            seam.is_ok() == production.is_ok(),
            "a {frame_len}-byte frame under a max of 8"
        );
    }
}
