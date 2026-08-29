//! A freeze record's signed bytes, rebuilt from the published layout.
//!
//! An auditor holding the audit topic and an operator's public key has to be
//! able to check a freeze signature with no broker in the loop. This module is
//! that auditor's side of the work, so the case that signs a request with the
//! preimage and the case that re-verifies a fetched record against it both
//! agree with the *document* rather than with the broker.

use krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED};

/// The domain separator in front of a freeze record's signed bytes.
///
/// KFC-9 publishes this constant and the layout below it, which is what lets
/// an auditor write their own verifier. This is that auditor's copy, written
/// from the specification rather than reached for inside the broker.
const FREEZE_DOMAIN: &[u8] = b"krabka-topic-freeze-v1\0";

/// Append `bytes` behind its `u32` big-endian length.
fn put_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("a fixture field is far below u32::MAX");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// The fields a freeze record's Ed25519 signature covers.
#[derive(Clone, Copy)]
pub(super) struct FreezeBytes<'a> {
    pub(super) cluster_id: &'a str,
    pub(super) pattern_type: i8,
    pub(super) scope: &'a str,
    pub(super) frozen: bool,
    pub(super) reason: &'a str,
    pub(super) set_by: &'a str,
    pub(super) set_at_ms: i64,
    pub(super) proposal_id: uuid::Uuid,
}

/// The canonical bytes of a freeze record, built the way an auditor builds
/// them: from the layout KFC-9 publishes, with no broker code in the loop.
///
/// A test that reached for the broker's own `freeze_signing_bytes` would prove
/// only that the broker agrees with itself. The point of this second
/// implementation is that it agrees with the *document*.
pub(super) fn freeze_signing_bytes(input: &FreezeBytes<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(FREEZE_DOMAIN);
    put_len_prefixed(&mut out, input.cluster_id.as_bytes());
    out.extend_from_slice(&input.pattern_type.to_be_bytes());
    put_len_prefixed(&mut out, input.scope.as_bytes());
    out.push(u8::from(input.frozen));
    put_len_prefixed(&mut out, input.reason.as_bytes());
    put_len_prefixed(&mut out, input.set_by.as_bytes());
    out.extend_from_slice(&input.set_at_ms.to_be_bytes());
    out.extend_from_slice(input.proposal_id.as_bytes());
    out
}

/// The `pattern_type` byte behind the `"<pattern>:"` prefix of an audit
/// event's target.
pub(super) fn pattern_type_byte(name: &str) -> i8 {
    match name {
        "literal" => PATTERN_TYPE_LITERAL,
        "prefixed" => PATTERN_TYPE_PREFIXED,
        other => panic!("no freeze pattern type is spelled {other:?}"),
    }
}
