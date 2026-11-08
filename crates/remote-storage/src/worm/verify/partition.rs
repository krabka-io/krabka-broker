//! One partition directory, from its objects to its report.
//!
//! This is the step between the archive-wide listing and the chain walk. It
//! decodes the directory's manifests, applies the request's topic and partition
//! filters, accounts for the objects no manifest names, and hands the rest to
//! the walk. A directory whose manifests do not all decode never reaches the
//! walk: the refusal is the finding.

use {
    super::{
        TrustedManifestKeys, VerifyRequest,
        listing::{DirListing, orphans},
        manifest_read::{KeyedManifest, ManifestRead, read_manifest},
        report::{VerifiedPartition, VerifyBreak, broken_before_walk},
        walk::walk_partition,
    },
    crate::worm::{
        error::WormError,
        manifest::{MANIFEST_SUFFIX, ManifestBody},
    },
    object_store::ObjectStore,
    std::{collections::BTreeMap, sync::Arc},
};

/// Verifies one partition directory, or `None` when the request filters it out.
pub(super) async fn verify_partition(
    store: &Arc<dyn ObjectStore>,
    dir: &str,
    listing: &DirListing,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<Option<VerifiedPartition>, WormError> {
    let mut decoded: Vec<KeyedManifest> = Vec::new();
    let mut rejected: Option<VerifyBreak> = None;
    for key in listing.keys().filter(|key| key.ends_with(MANIFEST_SUFFIX)) {
        match read_manifest(store, key).await? {
            ManifestRead::Decoded(manifest) => decoded.push(KeyedManifest {
                key: key.clone(),
                manifest: *manifest,
            }),
            ManifestRead::Rejected(reason) => {
                rejected.get_or_insert_with(|| VerifyBreak {
                    manifest_key: key.clone(),
                    seq: None,
                    reason,
                });
            }
        }
    }

    // A filter reads the topic and partition from a decoded manifest. When
    // every manifest was rejected there is none to read them from, so the
    // filter cannot rule the directory out, and it is reported rather than
    // dropped: a silent skip would read as a clean archive.
    let scope = decoded.first().map(|keyed| &keyed.manifest.body);
    let out_of_scope_unknown = scope.is_none() && rejected.is_some();
    if !out_of_scope_unknown && !selected(request, scope) {
        return Ok(None);
    }

    let orphan_objects = orphans(listing, &decoded);
    if let Some(first_break) = rejected {
        return Ok(Some(VerifiedPartition {
            report: broken_before_walk(dir, orphan_objects, first_break),
            objects: BTreeMap::new(),
        }));
    }

    let walk = walk_partition(store, &decoded, listing, request, trusted).await?;
    Ok(Some(walk.into_report(dir, orphan_objects, request)))
}

/// Whether the request's topic and partition filters admit this directory.
///
/// The filter reads the manifest and not the directory name. A directory name
/// embeds a URL-safe Base64 topic id, whose alphabet contains the same `-` that
/// separates the name's fields, so the name cannot be split back apart.
fn selected(request: &VerifyRequest, body: Option<&ManifestBody>) -> bool {
    if request.topic.is_none() && request.partition.is_none() {
        return true;
    }
    let Some(body) = body else { return false };
    request
        .topic
        .as_ref()
        .is_none_or(|topic| *topic == body.segment.topic)
        && request
            .partition
            .is_none_or(|partition| partition == body.segment.partition)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::verify::{
            test_support::{Archive, PARTITION, TOPIC},
            verify_archive,
        },
        assert2::check,
        krabka_object_store::fault::FaultKind,
    };

    #[tokio::test]
    async fn a_topic_filter_skips_the_partitions_it_does_not_name() {
        let archive = Archive::build(&[2]).await;
        let trusted = archive.trusted();

        let other_topic = VerifyRequest {
            topic: Some("payments".to_owned()),
            ..Default::default()
        };
        check!(
            verify_archive(&archive.store, &other_topic, &trusted)
                .await
                .unwrap()
                .partitions
                .is_empty()
        );

        let this_topic = VerifyRequest {
            topic: Some(TOPIC.to_owned()),
            partition: Some(PARTITION),
            ..Default::default()
        };
        check!(
            verify_archive(&archive.store, &this_topic, &trusted)
                .await
                .unwrap()
                .manifests()
                == 2
        );
    }

    #[yare::parameterized(
        other_topic = { Some("payments"), None },
        other_partition = { None, Some(PARTITION + 1) },
        this_partition = { Some(TOPIC), Some(PARTITION) },
    )]
    #[test_macro(tokio::test)]
    async fn a_filter_cannot_skip_a_partition_whose_every_manifest_is_missing(
        topic: Option<&str>,
        partition: Option<i32>,
    ) {
        let archive = Archive::with_failing_reads(FaultKind::NotFound).await;
        let request = VerifyRequest {
            topic: topic.map(str::to_owned),
            partition,
            ..Default::default()
        };

        let report = archive.verify_with(&request).await;

        check!(!report.ok());
        check!(
            report
                .first_break()
                .map(|found| found.manifest_key.as_str())
                == Some(archive.segments[0].manifest_key.as_str())
        );
    }
}
