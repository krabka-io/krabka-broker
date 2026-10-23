//! Trust policy for restore archives, captures, and retained snapshots.
use {
    crate::{
        ArchiveInventory, ArchiveStore, AuthenticationReport, RestoreArgs, RestoreError,
        SkippedSegment,
    },
    krabka_remote_storage::{
        AuthenticatedArchive, ObjectEntry, Sha256Digest, TrustedManifestKeys, VerifyRequest,
        authenticate_archive,
        diskless::{CAPTURE_HEAD_NAME, DisklessWalCapture},
        manifest_head,
    },
    krabka_remote_storage_topic::Snapshot,
    object_store::path::Path as ObjectPath,
    std::collections::BTreeMap,
};

/// Inputs accepted by the restore trust policy before target writes.
pub(super) struct AuthenticatedInputs {
    pub(super) source: Option<SourceAuthentication>,
    pub(super) capture: Option<CaptureAuthentication>,
    pub(super) snapshots: AuthenticatedSnapshots,
    pub(super) has_classic: bool,
    pub(super) unclaimed: Vec<SkippedSegment>,
}
pub(super) async fn authenticate_inputs(
    store: &ArchiveStore,
    args: &RestoreArgs,
    archive: &mut ArchiveInventory,
    capture: Option<&DisklessWalCapture>,
) -> Result<AuthenticatedInputs, RestoreError> {
    let trusted = trusted_keys(args)?;
    let has_classic = archive
        .partitions
        .iter()
        .any(|partition| !partition.segments.is_empty())
        || args.worm_expect_head.iter().any(|value| {
            value
                .split_once('=')
                .is_some_and(|(name, _)| name != CAPTURE_HEAD_NAME)
        });
    let diskless_authentication = match (&capture, &trusted) {
        (Some(capture), Some(trusted)) => Some(authenticate_capture(capture, &trusted.keys, args)?),
        (None, _) => {
            if args.worm_expect_head.iter().any(|value| {
                value
                    .split_once('=')
                    .is_some_and(|(name, _)| name == CAPTURE_HEAD_NAME)
            }) {
                return Err(RestoreError::Authenticity {
                    reason: "a diskless-capture head was pinned but no --diskless-wal-capture was supplied".to_owned(),
                });
            }
            None
        }
        (Some(_), None) => None,
    };
    let diskless_claims = diskless_authentication
        .as_ref()
        .map(|authentication| &authentication.claims);
    let mut authenticated = match (&trusted, has_classic) {
        (Some(trusted), true) => authenticate_source(store, args, trusted, diskless_claims).await?,
        _ => None,
    };
    if let Some(CaptureAuthentication { head, .. }) = &diskless_authentication {
        let count = trusted.as_ref().map_or(0, |trusted| trusted.count);
        record_capture_head(&mut authenticated, head, count);
    }
    // Before anything is seeded from the inventory: a segment no manifest
    // names, and a partition that held nothing else, are not restored.
    let unclaimed = drop_unclaimed_uploads(archive, authenticated.as_ref());
    let snapshots = authenticate_snapshots(args, capture, trusted.is_some()).await?;
    Ok(AuthenticatedInputs {
        source: authenticated,
        capture: diskless_authentication,
        snapshots,
        has_classic,
        unclaimed,
    })
}

pub(super) struct AuthenticatedSnapshots {
    pub(super) metadata: Option<Vec<u8>>,
    pub(super) rlmm: Option<Snapshot>,
}

async fn authenticate_snapshots(
    args: &RestoreArgs,
    capture: Option<&krabka_remote_storage::diskless::DisklessWalCapture>,
    authenticated_restore: bool,
) -> Result<AuthenticatedSnapshots, RestoreError> {
    let metadata = authenticate_captured_snapshot(
        args.archive.metadata_snapshot.as_deref(),
        capture.and_then(|capture| capture.metadata_snapshot_sha256),
        authenticated_restore,
        "--metadata-snapshot",
    )
    .await?;
    let rlmm_bytes = authenticate_captured_snapshot(
        args.archive.rlmm_snapshot.as_deref(),
        capture.and_then(|capture| capture.rlmm_snapshot_sha256),
        authenticated_restore,
        "--rlmm-snapshot",
    )
    .await?;
    let rlmm_snapshot = rlmm_bytes
        .as_deref()
        .map(|bytes| {
            Snapshot::decode(bytes).map_err(|error| {
                RestoreError::Integrity(format!(
                    "cannot decode authenticated RLMM snapshot: {error}"
                ))
            })
        })
        .transpose()?;
    Ok(AuthenticatedSnapshots {
        metadata,
        rlmm: rlmm_snapshot,
    })
}

pub(crate) async fn authenticate_captured_snapshot(
    path: Option<&std::path::Path>,
    expected: Option<Sha256Digest>,
    authenticated_restore: bool,
    flag: &str,
) -> Result<Option<Vec<u8>>, RestoreError> {
    let Some(path) = path else {
        // A signed capture that binds this input says the source had it. A
        // restore without it would silently come back without what it
        // carries: for the metadata checkpoint, every ACL and SCRAM credential.
        if let (true, Some(expected)) = (authenticated_restore, expected) {
            return Err(RestoreError::Authenticity {
                reason: format!(
                    "the signed diskless capture binds {flag} (SHA-256 {expected}), but it was \
                     not supplied"
                ),
            });
        }
        return Ok(None);
    };
    if !authenticated_restore {
        return Ok(None);
    }
    let expected = expected.ok_or_else(|| RestoreError::Authenticity {
        reason: format!("{flag} is not bound to the signed diskless capture"),
    })?;
    let bytes = tokio::fs::read(path).await?;
    let actual = Sha256Digest::of(&bytes);
    if actual != expected {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "{flag} differs from the signed capture: expected SHA-256 {expected}, found {actual}"
            ),
        });
    }
    Ok(Some(bytes))
}

/// Count the diskless capture's signed boundary in the authentication report,
/// creating the report when the archive held no classic segments.
fn record_capture_head(authenticated: &mut Option<SourceAuthentication>, head: &str, count: usize) {
    let source = authenticated.get_or_insert_with(|| SourceAuthentication {
        archive: None,
        unclaimed: std::collections::BTreeSet::new(),
        report: AuthenticationReport {
            partitions: 0,
            manifests: 0,
            objects: 0,
            trusted_keys: count,
            chain_heads: std::collections::BTreeMap::new(),
            tail_truncation_protected: true,
        },
    });
    source.report.partitions += 1;
    source.report.manifests += 1;
    source
        .report
        .chain_heads
        .insert(CAPTURE_HEAD_NAME.to_owned(), head.to_owned());
}

/// Remove from `archive` every segment whose objects an authenticated restore
/// found with no manifest naming them, and every partition left with no
/// segment, and say what was removed.
///
/// This runs before the inventory seeds anything. A partition whose only
/// upload was interrupted has no manifest and no head; were it kept, its
/// topic and partition would be created from directory names alone.
fn drop_unclaimed_uploads(
    archive: &mut ArchiveInventory,
    authenticated: Option<&SourceAuthentication>,
) -> Vec<SkippedSegment> {
    let Some(unclaimed) = authenticated.map(|source| &source.unclaimed) else {
        return Vec::new();
    };
    let mut skipped = Vec::new();
    for partition in &mut archive.partitions {
        partition.segments.retain(|segment| {
            let claimed = !segment
                .artifacts()
                .any(|object| unclaimed.contains(object.key.as_ref()));
            if !claimed {
                skipped.push(SkippedSegment {
                    topic: partition.partition.topic.clone(),
                    partition: partition.partition.partition,
                    segment_id: segment.segment_id,
                    reason: "uploaded without a manifest: no signed claim names it".to_owned(),
                });
            }
            claimed
        });
    }
    archive
        .partitions
        .retain(|partition| !partition.segments.is_empty());
    skipped
}

/// The trusted WORM manifest keys and how many the operator supplied.
struct TrustedKeySet {
    keys: TrustedManifestKeys,
    count: usize,
}

/// What authenticating the classic archive established: the authenticated
/// objects, when the archive holds classic segments, and the coverage the
/// report states.
pub(super) struct SourceAuthentication {
    pub(super) archive: Option<AuthenticatedArchive>,
    pub(super) report: AuthenticationReport,
    /// Objects inside a partition directory that no manifest names: an upload
    /// whose manifest was never written. They make no signed claim, so the
    /// restore leaves their segments out rather than trust them.
    unclaimed: std::collections::BTreeSet<String>,
}

async fn authenticate_source(
    store: &ArchiveStore,
    args: &RestoreArgs,
    trusted: &TrustedKeySet,
    diskless_claims: Option<
        &std::collections::BTreeMap<String, krabka_remote_storage::ObjectEntry>,
    >,
) -> Result<Option<SourceAuthentication>, RestoreError> {
    let request = VerifyRequest {
        prefix: store.prefix().map(str::to_owned),
        externally_authenticated_objects: diskless_claims
            .into_iter()
            .flat_map(|claims| claims.keys().cloned())
            .collect(),
        ..VerifyRequest::default()
    };
    let authenticated = authenticate_archive(store.store(), &request, &trusted.keys)
        .await
        .map_err(|error| RestoreError::Authenticity {
            reason: error.to_string(),
        })?;
    let report = authenticated.report();
    let reason = if report.partitions.is_empty() {
        Some("no WORM manifest chain found".to_owned())
    } else if let Some(found) = report.first_break() {
        Some(format!(
            "manifest `{}`: {}",
            found.manifest_key, found.reason
        ))
    } else if !report.fully_attested() {
        Some("one or more manifests are unsigned or use an untrusted key".to_owned())
    } else if !report.global_orphan_objects.is_empty() {
        Some("the archive contains objects outside its authenticated manifests".to_owned())
    } else if report.has_epoch_restarts() {
        Some("the archive contains an untrusted manifest-chain restart".to_owned())
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(RestoreError::Authenticity { reason });
    }
    let expected_heads = args
        .worm_expect_head
        .iter()
        .filter_map(|value| value.split_once('='))
        .filter(|(partition, _)| *partition != CAPTURE_HEAD_NAME)
        .map(|(partition, head)| (partition.to_owned(), head.to_ascii_lowercase()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let actual_heads = report
        .partitions
        .iter()
        .filter_map(|partition| {
            partition
                .head
                .map(|head| (partition.partition_dir.clone(), head.to_string()))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    if expected_heads != actual_heads {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "pinned WORM chain heads do not exactly cover authenticated partitions: expected {expected_heads:?}, found {actual_heads:?}"
            ),
        });
    }
    let coverage = AuthenticationReport {
        partitions: report.partitions.len(),
        manifests: report.manifests(),
        objects: u64::try_from(authenticated.objects().len()).unwrap_or(u64::MAX),
        trusted_keys: trusted.count,
        chain_heads: actual_heads,
        tail_truncation_protected: true,
    };
    // A partition whose chain is unbroken and fully attested is not `ok` only
    // for objects no manifest names. A source lost mid-upload leaves exactly
    // that: a segment's objects without the manifest that would have named
    // them. The pinned heads above already prove no manifest was taken away.
    let unclaimed = report
        .partitions
        .iter()
        .flat_map(|partition| partition.orphan_objects.iter().cloned())
        .collect();
    Ok(Some(SourceAuthentication {
        archive: Some(authenticated),
        report: coverage,
        unclaimed,
    }))
}

fn trusted_keys(args: &RestoreArgs) -> Result<Option<TrustedKeySet>, RestoreError> {
    if args.archive.worm_key_id.is_empty() {
        return Ok(None);
    }
    let pairs = args
        .archive
        .worm_key_id
        .iter()
        .zip(&args.archive.worm_public_key)
        .map(|(id, path)| {
            std::fs::read(path)
                .map(|key| (id.clone(), key))
                .map_err(|error| {
                    RestoreError::InvalidArgument(format!(
                        "cannot read --worm-public-key {}: {error}",
                        path.display()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let count = pairs.len();
    Ok(Some(TrustedKeySet {
        keys: TrustedManifestKeys::from_pairs(pairs),
        count,
    }))
}

/// What authenticating a diskless capture established.
pub(super) struct CaptureAuthentication {
    /// The objects the signed capture vouches for, by key.
    pub(super) claims: BTreeMap<String, ObjectEntry>,
    /// The head of the capture's WORM chain.
    pub(super) head: String,
}

fn authenticate_capture(
    capture: &DisklessWalCapture,
    trusted: &TrustedManifestKeys,
    args: &RestoreArgs,
) -> Result<CaptureAuthentication, RestoreError> {
    let expected = args.worm_expect_head.iter().find_map(|value| {
        value
            .split_once('=')
            .filter(|(name, _)| *name == CAPTURE_HEAD_NAME)
            .map(|(_, head)| head)
    });
    let claims = capture
        .authenticate(trusted, expected)
        .map_err(|reason| RestoreError::Authenticity { reason })?;
    let head = capture
        .authentication
        .as_ref()
        .map(|manifest| manifest_head(&manifest.body).to_string())
        .ok_or_else(|| RestoreError::Authenticity {
            reason: "diskless capture has no signed WORM boundary".to_owned(),
        })?;
    Ok(CaptureAuthentication { claims, head })
}

pub(super) fn object_claim<'a>(
    key: &ObjectPath,
    authenticated: Option<&'a BTreeMap<String, ObjectEntry>>,
) -> Result<Option<&'a ObjectEntry>, RestoreError> {
    let expected = authenticated.and_then(|objects| objects.get(&key.to_string()));
    if authenticated.is_some() && expected.is_none() {
        return Err(RestoreError::Authenticity {
            reason: format!("object `{key}` is not named by an authenticated manifest"),
        });
    }
    Ok(expected)
}
pub(super) fn verify_object_size(
    key: &ObjectPath,
    expected: Option<&ObjectEntry>,
    size: u64,
) -> Result<(), RestoreError> {
    if let Some(expected) = expected
        && size != expected.size_bytes
    {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "object `{key}` changed after manifest verification: expected {} bytes, found {} bytes",
                expected.size_bytes, size
            ),
        });
    }
    Ok(())
}
pub(super) fn verify_object_bytes(
    key: &ObjectPath,
    expected: Option<&ObjectEntry>,
    bytes: &[u8],
) -> Result<(), RestoreError> {
    if let Some(expected) = expected {
        let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let actual_digest = Sha256Digest::of(bytes);
        if expected.size_bytes != actual_size || expected.sha256 != actual_digest {
            return Err(RestoreError::Authenticity {
                reason: format!(
                    "object `{key}` changed after manifest verification: expected {} bytes with SHA-256 {}, fetched {actual_size} bytes with SHA-256 {actual_digest}",
                    expected.size_bytes, expected.sha256
                ),
            });
        }
    }
    Ok(())
}
