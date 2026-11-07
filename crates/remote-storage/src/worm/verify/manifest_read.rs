//! Getting one manifest out of an attacker-controlled object.
//!
//! Every manifest the verifier reads comes from the archive it is auditing, so
//! this module is the boundary that decides whether an object is a manifest at
//! all. It caps the read, refuses a body it cannot decode, and refuses a format
//! version it does not implement, and it reports each refusal as archive
//! content rather than as a failure to look. A manifest the listing named and
//! the store no longer holds is archive content too: the archive lost it.

use {
    crate::worm::{
        error::WormError,
        manifest::{MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, SegmentManifest},
    },
    krabka_object_store::{ObjectStoreError, read_capped},
    object_store::{ObjectStore, path::Path},
    std::sync::Arc,
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
        // A manifest the listing named and the store does not hold is missing
        // from the archive. That is a finding about the archive, the same as
        // an oversized one, and not a failure to look.
        Err(ObjectStoreError::NotFound(_)) => {
            return Ok(ManifestRead::Rejected(format!(
                "manifest of segment `{}` is missing from the archive",
                segment_name(key)
            )));
        }
        // Access denied, a network fault, a throttled store: the verifier
        // could not look, which says nothing about the archive.
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
    if let Some(reason) = format_version_problem(manifest.body.format_version) {
        return Ok(ManifestRead::Rejected(reason));
    }
    Ok(ManifestRead::Decoded(Box::new(manifest)))
}

/// The segment a manifest key names: its file name without the manifest suffix.
fn segment_name(key: &str) -> &str {
    let file = key.rsplit_once('/').map_or(key, |(_, file)| file);
    file.strip_suffix(MANIFEST_SUFFIX).unwrap_or(file)
}

/// Why a manifest of `version` is refused, or `None` for
/// [`MANIFEST_FORMAT_VERSION`], the only version a 1.x verifier reads.
fn format_version_problem(version: u32) -> Option<String> {
    match version {
        MANIFEST_FORMAT_VERSION => None,
        1 => Some(format!(
            "manifest format version 1 predates krabka 1.0, whose verifier reads only version \
             {MANIFEST_FORMAT_VERSION}"
        )),
        other => Some(format!(
            "unsupported manifest format version {other}: this build reads only version \
             {MANIFEST_FORMAT_VERSION}"
        )),
    }
}

/// One manifest object and the key it was read from.
pub(super) struct KeyedManifest {
    pub key: String,
    pub manifest: SegmentManifest,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            storage_manager::segment_file_name,
            worm::verify::{
                VerifyRequest,
                report::VerifyBreak,
                test_support::{Archive, put_raw},
                verify_archive,
            },
        },
        assert2::{assert, check},
        bytes::Bytes,
        krabka_object_store::fault::{
            FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp,
        },
        object_store::memory::InMemory,
    };

    /// An archive whose listing still names every object while every read
    /// fails with `kind`: the store the verifier meets when objects vanish
    /// between the listing and the read, or when it may list but not read.
    async fn archive_whose_reads_fail(kind: FaultKind) -> Archive {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let archive = Archive::build_on(Arc::clone(&backing), &[1]).await;
        let failing: Arc<dyn ObjectStore> = Arc::new(FaultInjectingStore::new(
            backing,
            FaultPolicy::none().with(StoreOp::Get, OpFault::always(kind)),
        ));
        archive.with_store(failing)
    }

    #[yare::parameterized(
        pre_1_0 = {
            1,
            "manifest format version 1 predates krabka 1.0, whose verifier reads only version 2",
        },
        older = { 0, "unsupported manifest format version 0: this build reads only version 2" },
        future = { 3, "unsupported manifest format version 3: this build reads only version 2" },
    )]
    #[test_macro(tokio::test)]
    async fn a_manifest_of_any_version_but_the_current_one_is_a_break(
        version: u64,
        expected: &str,
    ) {
        let archive = Archive::build(&[1]).await;
        let segment = &archive.segments[0];
        let mut value = serde_json::to_value(&segment.manifest).unwrap();
        value["body"]["format_version"] = serde_json::json!(version);
        put_raw(
            &archive.ops,
            &segment.manifest_key,
            Bytes::from(serde_json::to_vec(&value).unwrap()),
        )
        .await;

        let report = archive.verify().await;

        check!(report.first_break().map(|found| found.reason.as_str()) == Some(expected));
    }

    #[test]
    fn only_the_current_manifest_version_is_read() {
        check!(MANIFEST_FORMAT_VERSION == 2);
        check!(format_version_problem(MANIFEST_FORMAT_VERSION) == None);
    }

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

        let report = archive.verify().await;

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

    #[tokio::test]
    async fn a_missing_manifest_object_is_a_break_and_not_an_error() {
        let archive = archive_whose_reads_fail(FaultKind::NotFound).await;
        let segment = &archive.segments[0];

        let report = archive.verify().await;

        check!(!report.ok());
        let expected = VerifyBreak {
            manifest_key: segment.manifest_key.clone(),
            seq: None,
            reason: format!(
                "manifest of segment `{}` is missing from the archive",
                segment_file_name(&segment.metadata, "")
            ),
        };
        check!(report.first_break() == Some(&expected));
    }

    #[tokio::test]
    async fn a_manifest_read_the_store_refuses_is_an_error_and_not_a_break() {
        let archive = archive_whose_reads_fail(FaultKind::ServerError).await;

        let result = verify_archive(
            &archive.store,
            &VerifyRequest::default(),
            &archive.trusted(),
        )
        .await;

        assert!(let Err(WormError::Archive(message)) = result);
        check!(message.starts_with(&format!(
            "cannot read manifest `{}`",
            archive.segments[0].manifest_key
        )));
    }
}
