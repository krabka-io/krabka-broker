//! Digest verification of the objects a manifest names.
//!
//! A shallow run proves only that each object exists at the recorded size. A
//! deep run streams the body and recomputes its `SHA-256`, which is the one
//! check that sees a same-size substitution. On a versioned bucket the
//! mismatch also asks whether the version the manifest pinned still holds the
//! recorded bytes, because that is what separates a recoverable segment from a
//! lost one.

use {
    super::{VerifyDepth, listing::DirListing},
    crate::{
        storage_manager::parse_segment_file_name,
        worm::{
            error::WormError,
            manifest::{MANIFEST_SUFFIX, ObjectEntry, SegmentManifest, Sha256Digest},
        },
    },
    futures_util::TryStreamExt as _,
    krabka_verified::{
        WormDigestFacts, WormObjectAvailabilityFacts, WormObjectIdentityFacts,
        WormObjectSetDecision, WormObjectSetFacts, worm_object_set_decision,
    },
    object_store::{GetOptions, ObjectStore, path::Path},
    sha2::{Digest as _, Sha256},
    std::{collections::HashSet, sync::Arc},
};

fn object_is_in_manifest_segment(
    key: &str,
    manifest: &SegmentManifest,
    suffix: Option<&str>,
) -> bool {
    let segment = &manifest.body.segment;
    key.rsplit('/')
        .next()
        .and_then(parse_segment_file_name)
        .is_some_and(|parsed| {
            parsed.base_offset == segment.start_offset
                && parsed.segment_id == segment.segment_id
                && parsed.suffix != MANIFEST_SUFFIX
                && suffix.is_none_or(|suffix| parsed.suffix == suffix)
        })
}

/// Checks every object a manifest names, to the requested depth.
///
/// An object must live in the manifest's own partition directory. A manifest
/// that points somewhere else names an object this walk cannot account for, and
/// it counts as missing.
pub(super) async fn check_objects(
    store: &Arc<dyn ObjectStore>,
    manifest: &SegmentManifest,
    listing: &DirListing,
    depth: VerifyDepth,
) -> Result<Option<String>, WormError> {
    let listed_count = listing
        .keys()
        .filter(|key| object_is_in_manifest_segment(key, manifest, None))
        .count();
    let mut keys = HashSet::new();
    let mut duplicate = FirstFailure::default();
    let mut coordinate = FirstFailure::default();
    let mut missing = FirstFailure::default();
    let mut wrong_size = FirstFailure::default();
    let mut wrong_digest = FirstFailure::default();

    for object in &manifest.body.objects {
        if !keys.insert(object.key.as_str()) {
            duplicate.record(|| {
                format!(
                    "object `{}` is named more than once by the manifest",
                    object.key
                )
            });
        }

        let coordinate_matches =
            object_is_in_manifest_segment(&object.key, manifest, Some(&object.suffix));
        if !coordinate_matches {
            coordinate.record(|| {
                format!(
                    "object `{}` does not match the manifest segment coordinates or suffix",
                    object.key
                )
            });
        }

        match listing.get(&object.key).copied() {
            None => {
                missing.record(|| {
                    format!(
                        "object `{}` named by the manifest is missing from the archive",
                        object.key
                    )
                });
            }
            Some(size) => {
                if size != object.size_bytes {
                    wrong_size.record(|| {
                        format!(
                            "object `{}` is {size} bytes, the manifest records a size of {} bytes",
                            object.key, object.size_bytes
                        )
                    });
                }
                if depth == VerifyDepth::Deep {
                    let digest = object_digest(store, &object.key, None).await?;
                    if digest != object.sha256 {
                        let pinned = pinned_version_note(store, object).await;
                        wrong_digest.record(|| {
                            format!(
                                "object `{}` hashes to {digest}, the manifest records {}{pinned}",
                                object.key, object.sha256
                            )
                        });
                    }
                }
            }
        }
    }

    let object_count = u64::try_from(manifest.body.objects.len()).unwrap_or(u64::MAX);
    let listed_count = u64::try_from(listed_count).unwrap_or(u64::MAX);
    // `worm_object_set_decision` names a failed check only when that check's
    // fact is false, and each fact is false only once its failure is recorded.
    let reason = match worm_object_set_decision(WormObjectSetFacts {
        object_count,
        listed_count,
        identity: WormObjectIdentityFacts {
            unique_keys: duplicate.passed(),
            coordinates_match: coordinate.passed(),
        },
        availability: WormObjectAvailabilityFacts {
            all_present: missing.passed(),
            sizes_match: wrong_size.passed(),
        },
        digests: WormDigestFacts {
            require_digests: depth == VerifyDepth::Deep,
            digests_match: wrong_digest.passed(),
        },
    }) {
        WormObjectSetDecision::Empty => Some("manifest names no segment objects".to_owned()),
        WormObjectSetDecision::DuplicateKey => duplicate.into_reason(),
        WormObjectSetDecision::CoordinateMismatch => coordinate.into_reason(),
        WormObjectSetDecision::MissingObject => missing.into_reason(),
        WormObjectSetDecision::CountMismatch => Some(format!(
            "manifest names {object_count} objects, but the archive holds {listed_count} objects for this segment"
        )),
        WormObjectSetDecision::SizeMismatch => wrong_size.into_reason(),
        WormObjectSetDecision::DigestMismatch => wrong_digest.into_reason(),
        WormObjectSetDecision::Admit => None,
    };
    Ok(reason)
}

/// The first failure one object-set check found, as its reason text.
///
/// Every object is checked, but only the first failure is reported, so a
/// later failure leaves the recorded reason as it is.
#[derive(Default)]
struct FirstFailure(Option<String>);

impl FirstFailure {
    /// Records `reason` unless an earlier failure was recorded already.
    fn record(&mut self, reason: impl FnOnce() -> String) { self.0.get_or_insert_with(reason); }

    /// No failure was recorded.
    fn passed(&self) -> bool { self.0.is_none() }

    /// The first recorded failure.
    fn into_reason(self) -> Option<String> { self.0 }
}

/// Streams one object and returns its `SHA-256`.
///
/// The body is hashed chunk by chunk and never buffered whole, so the memory
/// cost does not follow the object size.
///
/// `version` selects one stored version of the key. `None` reads whatever the
/// key resolves to now, which is what the digest walk wants: the current
/// version is the one a reader gets, so it is the one that has to match.
async fn object_digest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    version: Option<&str>,
) -> Result<Sha256Digest, WormError> {
    let path = Path::from(key);
    let options = GetOptions {
        version: version.map(str::to_owned),
        ..GetOptions::default()
    };
    let fetch = store
        .get_opts(&path, options)
        .await
        .map_err(|e| WormError::Archive(format!("cannot read object `{key}`: {e}")))?;
    let mut stream = fetch.into_stream();
    let mut hasher = Sha256::new();
    loop {
        let next = stream
            .try_next()
            .await
            .map_err(|e| WormError::Archive(format!("cannot read object `{key}`: {e}")))?;
        let Some(chunk) = next else { break };
        hasher.update(&chunk);
    }
    Ok(Sha256Digest(hasher.finalize().into()))
}

/// Asks whether the version the manifest pinned still holds the bytes it
/// recorded, and says so in one clause appended to the mismatch.
///
/// An overwrite on a versioned bucket does not replace the locked original: it
/// stacks a new current version on top of it. [`object_digest`] reads the
/// current version, so the mismatch that brings us here is the detection this
/// feature exists for. What the mismatch alone does not say is whether the
/// archive is *recoverable*, and on an Object Lock bucket it usually is --
/// which is the difference between restoring a segment and declaring it lost.
///
/// Reading the pinned version is never the default. A walk that always read it
/// would confirm bytes no reader can reach by key any more, and would be blind
/// to the very overwrite this reports.
///
/// Supplementary, so it never fails the run: an unreadable pinned version is
/// reported in the clause rather than raised, because the mismatch is already
/// the finding.
async fn pinned_version_note(store: &Arc<dyn ObjectStore>, object: &ObjectEntry) -> String {
    let Some(version) = object.version_id.as_deref() else {
        return String::new();
    };
    match object_digest(store, &object.key, Some(version)).await {
        Ok(digest) if digest == object.sha256 => format!(
            ". The pinned version `{version}` still matches, so the original bytes survive \
             beneath the overwrite and the segment is recoverable"
        ),
        Ok(digest) => format!(
            ". The pinned version `{version}` hashes to {digest}, so it does not hold the \
             recorded bytes either"
        ),
        Err(e) => format!(". The pinned version `{version}` could not be read: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::verify::{
            VerifyRequest,
            test_support::{
                Archive, PREFIX, Tamper, VersionedStore, first_break_reason, put_entry, put_raw,
            },
        },
        assert2::check,
        bytes::Bytes,
        krabka_object_store::ObjectStoreClient,
    };

    async fn overwrite_log_body(archive: &Archive) {
        let segment = &archive.segments[0];
        let swapped: Vec<u8> = segment.log_body.iter().map(|b| b ^ 0xff).collect();
        check!(swapped.len() == segment.log_body.len());
        put_raw(&archive.ops, &segment.log_key, Bytes::from(swapped)).await;
    }

    /// Overwriting a body on a versioned bucket does not remove the bytes the
    /// manifest recorded: it stacks a new current version over a locked one.
    /// The walk must still fail -- a reader gets the current version -- and the
    /// verdict must say the original survives, because that is the difference
    /// between restoring the segment and writing it off.
    #[tokio::test]
    async fn a_deep_digest_mismatch_says_when_the_pinned_version_still_holds() {
        let archive = Archive::build_on(Arc::new(VersionedStore::new()), &[1]).await;
        let segment = &archive.segments[0];
        check!(
            segment.entries.iter().all(|e| e.version_id.is_some()),
            "the fixture store must record a version per object"
        );

        // Same length, different bytes: the case only a deep run catches.
        overwrite_log_body(&archive).await;

        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..VerifyRequest::default()
        };
        let report = archive.verify_with(&request).await;

        check!(!report.ok(), "an overwritten body is a break");
        let reason = first_break_reason(&report);
        check!(reason.contains("hashes to"), "reason was: {reason}");
        check!(
            reason.contains("still matches") && reason.contains("recoverable"),
            "the verdict must say the locked original survives. reason was: {reason}"
        );
    }

    /// The same overwrite on a bucket without versioning. There is no version
    /// to pin, so the mismatch stands alone rather than growing a clause that
    /// claims a recovery nobody can perform.
    #[tokio::test]
    async fn a_deep_digest_mismatch_adds_no_note_without_versioning() {
        let archive = Archive::build(&[1]).await;
        let segment = &archive.segments[0];
        check!(
            segment.entries.iter().all(|e| e.version_id.is_none()),
            "InMemory records no versions, which is the point of this row"
        );

        overwrite_log_body(&archive).await;

        let deep_req = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..Default::default()
        };
        let report = archive.verify_with(&deep_req).await;

        check!(!report.ok());
        let reason = first_break_reason(&report);
        check!(reason.contains("hashes to"), "reason was: {reason}");
        check!(
            !reason.contains("pinned version"),
            "nothing was pinned, so nothing may be claimed about one. reason was: {reason}"
        );
    }

    /// A version the manifest names but the bucket no longer holds. The note
    /// reports it and the run carries on: the digest mismatch is already the
    /// finding, and a failed lookup for the supplementary answer must not
    /// become an error of its own.
    #[tokio::test]
    async fn pinned_version_note_reports_a_version_that_cannot_be_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(VersionedStore::new());
        let ops = ObjectStoreClient::new(Arc::clone(&store));
        let key = format!("{PREFIX}/gone.log");
        let entry = put_entry(&ops, ".log", &key, b"body").await;
        let missing = ObjectEntry {
            version_id: Some("v-never-written".to_owned()),
            ..entry
        };

        let note = pinned_version_note(&store, &missing).await;

        check!(note.contains("v-never-written"), "note was: {note}");
        check!(note.contains("could not be read"), "note was: {note}");
    }
    /// The key of the `.log` object of the fixture's one segment.
    fn log_key(archive: &Archive) -> &str { &archive.segments[0].log_key }

    /// The `SHA-256` of the fixture's `.log` body with its first byte flipped,
    /// and of the body the manifest recorded, as the reason spells them.
    fn flipped_and_recorded_digests(archive: &Archive) -> (String, String) {
        let body = &archive.segments[0].log_body;
        let mut flipped = body.clone();
        flipped[0] ^= 0xff;
        (
            hex::encode(Sha256Digest::of(&flipped).0),
            hex::encode(Sha256Digest::of(body).0),
        )
    }

    /// The reason text for every outcome of the object-set check, each
    /// reached through a whole verify run over a one-segment archive whose
    /// `.log` body is the 18 bytes `segment-0-log-body`.
    #[yare::parameterized(
        admit_shallow = { false, Tamper::None, VerifyDepth::Shallow, |_| None },
        admit_deep = { false, Tamper::None, VerifyDepth::Deep, |_| None },
        empty = {
            false,
            Tamper::EmptyObjects(0),
            VerifyDepth::Shallow,
            |_| Some("manifest names no segment objects".to_owned()),
        },
        duplicate_key = {
            false,
            Tamper::DuplicateObject(0),
            VerifyDepth::Shallow,
            |archive| Some(format!(
                "object `{}` is named more than once by the manifest",
                log_key(archive)
            )),
        },
        coordinate_mismatch = {
            false,
            Tamper::WrongObjectCoordinates(0),
            VerifyDepth::Shallow,
            |archive| Some(format!(
                "object `{}` does not match the manifest segment coordinates or suffix",
                log_key(archive)
            )),
        },
        missing_object = {
            false,
            Tamper::DeleteLog(0),
            VerifyDepth::Shallow,
            |archive| Some(format!(
                "object `{}` named by the manifest is missing from the archive",
                log_key(archive)
            )),
        },
        count_mismatch = {
            false,
            Tamper::DropObjectEntry(0),
            VerifyDepth::Shallow,
            |_| Some(
                "manifest names 1 objects, but the archive holds 2 objects for this segment"
                    .to_owned()
            ),
        },
        size_mismatch = {
            false,
            Tamper::TruncateLog(0),
            VerifyDepth::Shallow,
            |archive| Some(format!(
                "object `{}` is 17 bytes, the manifest records a size of 18 bytes",
                log_key(archive)
            )),
        },
        digest_mismatch = {
            false,
            Tamper::FlipLogByte(0),
            VerifyDepth::Deep,
            |archive| {
                let (flipped, recorded) = flipped_and_recorded_digests(archive);
                Some(format!(
                    "object `{}` hashes to {flipped}, the manifest records {recorded}",
                    log_key(archive)
                ))
            },
        },
        digest_mismatch_with_a_surviving_pinned_version = {
            true,
            Tamper::FlipLogByte(0),
            VerifyDepth::Deep,
            |archive| {
                let (flipped, recorded) = flipped_and_recorded_digests(archive);
                let version = archive.segments[0].entries[0]
                    .version_id
                    .clone()
                    .expect("the versioned store records a version per object");
                Some(format!(
                    "object `{}` hashes to {flipped}, the manifest records {recorded}. The \
                     pinned version `{version}` still matches, so the original bytes survive \
                     beneath the overwrite and the segment is recoverable",
                    log_key(archive)
                ))
            },
        },
    )]
    #[test_macro(tokio::test)]
    async fn every_object_set_decision_reports_its_exact_reason(
        versioned: bool,
        tamper: Tamper,
        depth: VerifyDepth,
        expected: fn(&Archive) -> Option<String>,
    ) {
        let archive = if versioned {
            Archive::build_on(Arc::new(VersionedStore::new()), &[1]).await
        } else {
            Archive::build(&[1]).await
        };
        tamper.apply(&archive).await;

        let request = VerifyRequest {
            depth,
            ..VerifyRequest::default()
        };
        let report = archive.verify_with(&request).await;

        check!(report.first_break().map(|found| found.reason.clone()) == expected(&archive));
    }
}
