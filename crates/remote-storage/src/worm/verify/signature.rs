//! What one manifest's signature is worth to a verification run.
//!
//! The check is separate from the chain walk because it grades three outcomes
//! the walk treats differently: a manifest with no signature and a manifest
//! signed by an untrusted `key_id` are counted and the walk continues, while a
//! signature that fails against a trusted key stops the walk.

use super::TrustedManifestKeys;
use crate::worm::manifest::{SegmentManifest, verify_manifest_signature};

/// What one manifest's signature is worth to this run.
pub(super) enum SignatureState {
    /// No signature at all.
    Unsigned,
    /// Signed by a `key_id` this run does not trust.
    Untrusted,
    /// Signed by a trusted key, and the signature verifies.
    Valid,
    /// Signed by a trusted key, and the signature does not verify.
    Invalid(String),
}

/// Checks one manifest's signature against the trusted key it names.
pub(super) fn signature_state(
    manifest: &SegmentManifest,
    trusted: &TrustedManifestKeys,
) -> SignatureState {
    let Some(signature) = manifest.signature.as_ref() else {
        return SignatureState::Unsigned;
    };
    let Some(public_key) = trusted.get(&signature.key_id) else {
        return SignatureState::Untrusted;
    };
    if verify_manifest_signature(manifest, public_key) {
        SignatureState::Valid
    } else {
        SignatureState::Invalid(format!(
            "signature does not verify against the trusted key `{}`",
            signature.key_id
        ))
    }
}
