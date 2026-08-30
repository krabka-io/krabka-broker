//! Getting one manifest out of an attacker-controlled object.
//!
//! Every manifest the verifier reads comes from the archive it is auditing, so
//! this module is the boundary that decides whether an object is a manifest at
//! all. It caps the read, refuses a body it cannot decode, and refuses a format
//! version it does not implement, and it reports each refusal as archive
//! content rather than as a failure to look.

use std::sync::Arc;

use krabka_object_store::{ObjectStoreError, read_capped};
use object_store::{ObjectStore, path::Path};

use crate::worm::{
    error::WormError,
    manifest::{MANIFEST_FORMAT_VERSION, SegmentManifest},
};

/// Byte cap on one manifest object.
///
/// A manifest describes a single segment copy, so a real one is a few
/// kilobytes. The cap is what stops a hostile archive from making the verifier
/// buffer an arbitrary object: [`read_capped`] issues a `HEAD` first and
/// refuses an oversized object before it reads a byte of it.
pub const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

/// One manifest object, as far as the verifier could take it.
pub(super) enum ManifestRead {
    /// The object decoded into a manifest of a supported format version.
    Decoded(Box<SegmentManifest>),
    /// The object is not a manifest this verifier accepts, and why.
    Rejected(String),
}

/// Reads and decodes one manifest object.
pub(super) async fn read_manifest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<ManifestRead, WormError> {
    let bytes = match read_capped(store, &Path::from(key), MAX_MANIFEST_BYTES).await {
        Ok(bytes) => bytes,
        // An oversized object is archive content and not a failure to look, so
        // it grades as a break rather than as an error.
        Err(ObjectStoreError::TooLarge {
            size, max_bytes, ..
        }) => {
            return Ok(ManifestRead::Rejected(format!(
                "manifest object is {size} bytes, above the {max_bytes} byte cap"
            )));
        }
        Err(e) => {
            return Err(WormError::Archive(format!(
                "cannot read manifest `{key}`: {e}"
            )));
        }
    };
    let manifest: SegmentManifest = match serde_json::from_slice(&bytes) {
        Ok(manifest) => manifest,
        Err(e) => {
            return Ok(ManifestRead::Rejected(format!(
                "manifest does not decode: {e}"
            )));
        }
    };
    if !(1..=MANIFEST_FORMAT_VERSION).contains(&manifest.body.format_version) {
        return Ok(ManifestRead::Rejected(format!(
            "manifest format version {} is outside the supported range 1..={MANIFEST_FORMAT_VERSION}",
            manifest.body.format_version
        )));
    }
    Ok(ManifestRead::Decoded(Box::new(manifest)))
}

/// One manifest object and the key it was read from.
pub(super) type KeyedManifest = (String, SegmentManifest);

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;

    use super::*;
    use crate::worm::verify::{
        VerifyRequest,
        test_support::{Archive, put_raw},
        verify_archive,
    };

    #[tokio::test]
    async fn an_oversized_manifest_object_is_a_break_and_not_an_error() {
        let archive = Archive::build(&[1]).await;
        let huge = vec![b'{'; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1];
        put_raw(
            &archive.ops,
            &archive.segments[0].manifest_key,
            Bytes::from(huge),
        )
        .await;

        let report = verify_archive(
            &archive.store,
            &VerifyRequest::default(),
            &archive.trusted(),
        )
        .await
        .unwrap();

        check!(!report.ok());
        match report.first_break() {
            Some(found) => {
                check!(found.reason.contains("above the"));
            }
            None => {
                check!(false, "an oversized manifest must record a break");
            }
        }
    }
}
