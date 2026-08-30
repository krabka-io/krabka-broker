//! The chain walk: the part of a run that decides whether a partition's
//! manifests form one unbroken, correctly signed, fully backed sequence.
//!
//! The walk groups the partition's manifests into chain runs, replays each run
//! from genesis, and stops at the first manifest that does not continue it. It
//! accumulates what the partition's report needs as it goes, so a walk that
//! stops early still describes everything it verified before the break.

use std::{collections::BTreeMap, sync::Arc};

use object_store::ObjectStore;

use super::{
    TrustedManifestKeys, VerifyRequest,
    listing::DirListing,
    manifest_read::KeyedManifest,
    objects::check_objects,
    report::{
        EpochSpan, ObjectProtectionReport, PartitionVerifyReport, VerifyBreak, extend_span,
        offset_gaps,
    },
    signature::{SignatureState, signature_state},
};
use crate::worm::{
    error::WormError,
    manifest::{ChainHead, EpochId, ManifestSeq, SegmentManifest, manifest_head},
};

/// Everything the chain walk accumulates for one partition.
#[derive(Default)]
pub(super) struct Walk {
    manifests: u64,
    objects_checked: u64,
    create_precondition_objects: ObjectProtectionReport,
    bucket_retention_objects: ObjectProtectionReport,
    unknown_protection_objects: ObjectProtectionReport,
    unsigned: u64,
    untrusted: u64,
    epochs: Vec<EpochSpan>,
    segments: Vec<(i64, i64)>,
    head: Option<ChainHead>,
    last: Option<(String, ManifestSeq)>,
    first_break: Option<VerifyBreak>,
}

impl Walk {
    /// Records one manifest that passed every check.
    fn accept(&mut self, key: &str, manifest: &SegmentManifest, head: ChainHead) {
        let body = &manifest.body;
        self.manifests = self.manifests.saturating_add(1);
        self.objects_checked = self
            .objects_checked
            .saturating_add(u64::try_from(body.objects.len()).unwrap_or(u64::MAX));
        for object in &body.objects {
            if body.format_version == 1 {
                self.unknown_protection_objects.record(&object.key);
            } else if object.create_precondition {
                self.create_precondition_objects.record(&object.key);
            } else {
                self.bucket_retention_objects.record(&object.key);
            }
        }
        self.segments
            .push((body.segment.start_offset, body.segment.end_offset));
        self.head = Some(head);
        self.last = Some((key.to_string(), body.chain.seq));
    }

    /// Turns the accumulated walk into the partition's report.
    pub(super) fn into_report(
        mut self,
        dir: &str,
        orphan_objects: Vec<String>,
        request: &VerifyRequest,
    ) -> PartitionVerifyReport {
        let first_break = self
            .first_break
            .take()
            .or_else(|| self.tip_break(dir, request));
        PartitionVerifyReport {
            partition_dir: dir.to_string(),
            manifests: self.manifests,
            objects_checked: self.objects_checked,
            create_precondition_objects: self.create_precondition_objects,
            bucket_retention_objects: self.bucket_retention_objects,
            unknown_protection_objects: self.unknown_protection_objects,
            epochs: self.epochs,
            unsigned_manifests: self.unsigned,
            untrusted_manifests: self.untrusted,
            orphan_objects,
            offset_gaps: offset_gaps(&mut self.segments),
            head: self.head,
            ok: first_break.is_none(),
            first_break,
        }
    }

    /// The break that [`VerifyRequest::expect_head`] raises when the archive
    /// stops short of the head the caller obtained elsewhere.
    fn tip_break(&self, dir: &str, request: &VerifyRequest) -> Option<VerifyBreak> {
        let expected = request.expect_head?;
        let tip = self.head.unwrap_or(ChainHead::GENESIS);
        if tip == expected {
            return None;
        }
        // With nothing verified there is no manifest to point at, so the
        // break names the directory that should have held one.
        let (manifest_key, seq) = self.last.as_ref().map_or_else(
            || (dir.to_string(), None),
            |(key, seq)| (key.clone(), Some(*seq)),
        );
        Some(VerifyBreak {
            manifest_key,
            seq,
            reason: format!(
                "archive tip {tip} does not match the expected head {expected}: the archive \
                 stops short of a head obtained outside it, which is what tail truncation \
                 looks like"
            ),
        })
    }
}

/// Walks every chain run in the partition, stopping at the first break.
pub(super) async fn walk_partition(
    store: &Arc<dyn ObjectStore>,
    decoded: &[KeyedManifest],
    listing: &DirListing,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<Walk, WormError> {
    let mut walk = Walk::default();
    for run in chain_runs(decoded) {
        let mut span: Option<EpochSpan> = None;
        // Every run starts at genesis. A run that could not read its previous
        // head is a new run, so it never continues an older head.
        let mut head = ChainHead::GENESIS;
        let mut expected_seq = 0u64;
        for (key, manifest) in run {
            let body = &manifest.body;
            if let Some(reason) = chain_break(manifest, expected_seq, head) {
                walk.first_break = Some(VerifyBreak {
                    manifest_key: key.clone(),
                    seq: Some(body.chain.seq),
                    reason,
                });
                walk.epochs.extend(span);
                return Ok(walk);
            }
            head = manifest_head(body);
            expected_seq = body.chain.seq.0.saturating_add(1);

            match signature_state(manifest, trusted) {
                SignatureState::Unsigned => walk.unsigned = walk.unsigned.saturating_add(1),
                SignatureState::Untrusted => walk.untrusted = walk.untrusted.saturating_add(1),
                SignatureState::Valid => {}
                SignatureState::Invalid(reason) => {
                    walk.first_break = Some(VerifyBreak {
                        manifest_key: key.clone(),
                        seq: Some(body.chain.seq),
                        reason,
                    });
                    walk.epochs.extend(span);
                    return Ok(walk);
                }
            }

            if let Some(reason) = check_objects(store, manifest, listing, request.depth).await? {
                walk.first_break = Some(VerifyBreak {
                    manifest_key: key.clone(),
                    seq: Some(body.chain.seq),
                    reason,
                });
                walk.epochs.extend(span);
                return Ok(walk);
            }

            walk.accept(key, manifest, head);
            extend_span(&mut span, body, head);
        }
        walk.epochs.extend(span);
    }
    Ok(walk)
}

/// Groups the manifests into chain runs, ordered the way an archive grows.
///
/// Runs come out ordered by their lowest segment start offset, and the
/// manifests inside a run come out ordered by sequence. The object-store key
/// breaks a sequence tie, so two runs against an unchanged archive agree.
fn chain_runs(decoded: &[KeyedManifest]) -> Vec<Vec<&KeyedManifest>> {
    let mut runs: BTreeMap<EpochId, Vec<&KeyedManifest>> = BTreeMap::new();
    for entry in decoded {
        runs.entry(entry.1.body.chain.epoch_id)
            .or_default()
            .push(entry);
    }
    let mut ordered: Vec<Vec<&KeyedManifest>> = runs.into_values().collect();
    for run in &mut ordered {
        run.sort_by(|a, b| {
            a.1.body
                .chain
                .seq
                .cmp(&b.1.body.chain.seq)
                .then_with(|| a.0.cmp(&b.0))
        });
    }
    ordered.sort_by_key(|run| {
        let start = run
            .iter()
            .map(|(_, manifest)| manifest.body.segment.start_offset)
            .min()
            .unwrap_or(i64::MAX);
        (start, run.first().map(|(_, m)| m.body.chain.epoch_id))
    });
    ordered
}

/// Why the manifest does not continue the running chain, if it does not.
fn chain_break(manifest: &SegmentManifest, expected_seq: u64, head: ChainHead) -> Option<String> {
    let chain = manifest.body.chain;
    if chain.seq.0 != expected_seq {
        return Some(format!(
            "chain sequence gap: expected seq {expected_seq}, the manifest records seq {}",
            chain.seq
        ));
    }
    if chain.prev_head != head {
        return Some(format!(
            "chain head mismatch: the manifest records prev_head {}, the running head is {head}",
            chain.prev_head
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::worm::verify::{
        VerifyDepth,
        test_support::{Archive, Tamper},
        verify_archive,
    };

    #[tokio::test]
    async fn legacy_manifest_protection_is_unknown() {
        let archive = Archive::build(&[1]).await;
        let mut manifest = archive.segments[0].manifest.clone();
        manifest.body.format_version = 1;
        let mut walk = Walk::default();
        walk.accept("legacy.manifest", &manifest, manifest_head(&manifest.body));

        let report = walk.into_report("archive/topic-0-id", Vec::new(), &VerifyRequest::default());

        check!(report.unknown_protection_objects.count == 2);
        check!(report.create_precondition_objects.count == 0);
        check!(report.bucket_retention_objects.count == 0);
    }

    #[tokio::test]
    async fn verify_report_is_deterministic() {
        let archive = Archive::build(&[2, 2]).await;
        Tamper::StrayObject.apply(&archive).await;
        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..Default::default()
        };

        let first = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();
        let second = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();

        check!(first == second);
        check!(first.has_epoch_restarts());
    }
}
