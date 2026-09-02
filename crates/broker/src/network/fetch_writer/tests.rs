//! Unit tests for the framed plan the module root builds: the golden
//! comparison against the copying `encode_response` path, the frame-length
//! accounting of the leading prefix, and the configured frame maximum.

use krabka_protocol::Encode;

use super::{
    test_support::{DEFAULT_MAX_FRAME_BYTES, inline_bytes, sample_response},
    *,
};

/// The broker-level golden test: the full framed bytes that
/// `build_fetch_plan` produces, which are the length prefix, the
/// correlation header, and the body, must equal the bytes from the
/// `encode_response(encode_fetch_response(..))` path, for both
/// non-flexible and flexible versions.
#[test]
fn build_fetch_plan_matches_legacy_encode_path() {
    for version in [4i16, 7, 11, 12, 13, 16, 18] {
        let resp = sample_response(version);
        let correlation_id = 0x1234_5678;
        let body_flexible = version >= 12;

        // New path: assemble the plan bytes.
        let ops = build_fetch_plan(
            &resp,
            version,
            correlation_id,
            body_flexible,
            DEFAULT_MAX_FRAME_BYTES,
            resolve_records_inline,
        )
        .unwrap();
        let mut new_bytes = BytesMut::new();
        for op in &ops {
            match op {
                WriteOp::Inline(b) => new_bytes.extend_from_slice(b),
                #[cfg(any(
                    target_os = "linux",
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "freebsd",
                    target_os = "dragonfly",
                ))]
                WriteOp::File(_) => unreachable!("inline resolver emits no File ops"),
            }
        }

        // Old path: encode the body, then the response header, then frame.
        let mut body = BytesMut::new();
        resp.encode(&mut body, version).unwrap();
        let header_v1 = response_header_v1(ApiKey::Fetch as i16, body_flexible);
        let header_len = response_header_len(ApiKey::Fetch as i16, body_flexible);
        let frame_body_len = header_len + body.len();
        let mut old_bytes = BytesMut::new();
        old_bytes.put_u32(u32::try_from(frame_body_len).unwrap());
        old_bytes.put_i32(correlation_id);
        if header_v1 {
            old_bytes.put_u8(0);
        }
        old_bytes.extend_from_slice(&body);

        assert2::assert!(
            (&new_bytes[..]) == (&old_bytes[..]),
            "plan != legacy encode at version {version}"
        );
    }
}

#[test]
fn plan_total_len_matches_frame_prefix() {
    // The 4-byte frame prefix the writer emits must equal the actual bytes
    // following it (header + body). Off-by-one here corrupts every frame.
    for version in [4i16, 12, 18] {
        let resp = sample_response(version);
        let ops = build_fetch_plan(
            &resp,
            version,
            1,
            version >= 12,
            DEFAULT_MAX_FRAME_BYTES,
            resolve_records_inline,
        )
        .unwrap();
        // First op is [u32 len][header]; the declared length must equal the
        // sum of the remaining bytes of op0 (the header) + all later ops.
        let head = inline_bytes(&ops[0]);
        let declared = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let header_after_len = head.len() - 4;
        let tail_len: usize = ops[1..].iter().map(WriteOp::body_len).sum();
        assert2::assert!((declared) == (header_after_len + tail_len));
    }
}

#[test]
fn build_fetch_plan_honors_nondefault_max_frame_length() {
    let response = sample_response(12);
    let unconstrained =
        build_fetch_plan(&response, 12, 1, true, usize::MAX, resolve_records_inline)
            .expect("unconstrained plan");
    let head = inline_bytes(&unconstrained[0]);
    let frame_body_len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;

    assert2::assert!(
        build_fetch_plan(
            &response,
            12,
            1,
            true,
            frame_body_len,
            resolve_records_inline,
        )
        .is_ok(),
        "a frame equal to the configured maximum must be accepted"
    );
    assert2::assert!(
        build_fetch_plan(
            &response,
            12,
            1,
            true,
            frame_body_len - 1,
            resolve_records_inline,
        )
        .is_err(),
        "a frame above the configured maximum must be rejected"
    );
}
