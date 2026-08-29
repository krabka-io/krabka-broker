//! The six rules that a detached operator signature passes.
//!
//! The rules run in one order and stop at the first failure: the trust set
//! knows the `key_id`, the key speaks for the author the record claims, the
//! connection authenticated that same author, `set_at_ms` sits inside the skew
//! window, `set_at_ms` is newer than the entry the record replaces, and the
//! signature verifies over the canonical bytes.

use krabka_metadata::TopicFreezeRecord;
use krabka_units::{Time, convert::TimeExt as _};

use super::{canonical_bytes::freeze_signing_bytes, refusal::SignatureRefusal};
use crate::operator_keys::OperatorKeys;

/// Everything outside the record that a signature check reads.
pub(crate) struct FreezeSignatureCheck<'a> {
    /// The operator key trust set from `[[operator_keys]]`.
    pub keys: &'a OperatorKeys,
    /// The cluster this broker belongs to, in string form.
    pub cluster_id: &'a str,
    /// The principal the broker authenticated on the connection.
    pub connection_principal: &'a str,
    /// How far `set_at_ms` may sit from `now_ms`, from
    /// `freeze.signature_max_skew`.
    pub max_skew: Time,
    /// This broker's clock, in milliseconds since the Unix epoch.
    pub now_ms: i64,
    /// The live registry entry that the incoming record replaces, when there
    /// is one. Its `set_at_ms` is the floor that the incoming timestamp must
    /// pass.
    pub replaces: Option<&'a TopicFreezeRecord>,
}

/// Verify the detached operator signature that `record` carries.
///
/// The function is the one place that holds all six rules. It checks that the
/// trust set knows the `key_id`, that the claimed author is the principal
/// bound to that key, that the same author is the principal on the connection,
/// that `set_at_ms` sits inside the skew window, that `set_at_ms` is newer than
/// the entry the record replaces, and that the signature verifies over
/// [`freeze_signing_bytes`].
///
/// # Errors
///
/// Returns the [`SignatureRefusal`] of the first rule that fails. Every one of
/// them carries `OPERATOR_SIGNATURE_INVALID` (1009).
pub(crate) fn verify_freeze_signature(
    check: &FreezeSignatureCheck<'_>,
    record: &TopicFreezeRecord,
) -> Result<(), SignatureRefusal> {
    let key = check
        .keys
        .get(&record.key_id)
        .ok_or(SignatureRefusal::UnknownKeyId)?;
    if key.principal() != record.set_by {
        return Err(SignatureRefusal::AuthorIsNotTheKeyPrincipal);
    }
    if record.set_by != check.connection_principal {
        return Err(SignatureRefusal::AuthorIsNotTheConnectionPrincipal);
    }
    if !inside_skew_window(record.set_at_ms, check.now_ms, check.max_skew) {
        return Err(SignatureRefusal::TimestampOutsideSkewWindow);
    }
    if let Some(replaced) = check.replaces
        && record.set_at_ms <= replaced.set_at_ms
    {
        return Err(SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces);
    }
    let message = freeze_signing_bytes(check.cluster_id, record);
    if !check
        .keys
        .verify(&record.key_id, &record.set_by, &message, &record.signature)
    {
        return Err(SignatureRefusal::SignatureDidNotVerify);
    }
    Ok(())
}

/// Whether `set_at_ms` sits within `max_skew` of `now_ms`, in either
/// direction.
///
/// A record from the future is as suspect as one from the past, so the window
/// is symmetric. The subtraction saturates, which keeps a clock at the far end
/// of the `i64` range from wrapping into the window.
fn inside_skew_window(set_at_ms: i64, now_ms: i64, max_skew: Time) -> bool {
    let distance = now_ms.saturating_sub(set_at_ms).unsigned_abs();
    let window = u64::try_from(max_skew.millis_i64()).unwrap_or(0);
    distance <= window
}
