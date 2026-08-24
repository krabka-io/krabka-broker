//! Standalone verification of a WORM archive.
//!
//! The verifier reads an archive with nothing but a listing credential and the
//! public half of the signing key. It needs no broker, no metadata manager, and
//! no cluster, because everything it checks is written into the archive:
//! [`crate::SegmentManifest`] carries the object digests, the chain position,
//! and the signature. An auditor who holds a read-only role can therefore
//! confirm the archive years after the broker that wrote it stopped running.
//!
//! # What each depth proves
//!
//! [`VerifyDepth::Shallow`] recomputes the chain, checks every signature, and
//! confirms that every object a manifest names exists with the recorded size.
//! It never downloads a segment body, so it is cheap enough to run continuously
//! against a large archive. It cannot see a body that was replaced with
//! different bytes of the same length.
//!
//! [`VerifyDepth::Deep`] downloads every object and recomputes its `SHA-256`.
//! It is the only depth that catches a same-size substitution, and it reads the
//! whole archive to do so.
//!
//! # What no depth proves
//!
//! Tail truncation. An attacker who removes the newest manifests, and every
//! object they name, leaves a shorter chain that verifies perfectly. Give
//! [`VerifyRequest::expect_head`] a head obtained outside the archive to close
//! that gap.
//!
//! # Hostile input
//!
//! The archive is attacker-controlled. Every manifest read goes through
//! [`read_capped`] with the [`MAX_MANIFEST_BYTES`] cap, so an oversized object
//! is refused by a `HEAD` before a byte is buffered, and no allocation is ever
//! sized from a count a manifest supplies. A damaged or hostile archive
//! produces a report, never a panic.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use crabka_object_store::{ObjectStoreError, read_capped};
use futures_util::TryStreamExt as _;
use object_store::{GetOptions, ObjectStore, path::Path};
use sha2::{Digest as _, Sha256};

use crate::worm::{
    error::WormError,
    manifest::{
        ChainHead, EpochId, MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, ManifestBody, ManifestSeq,
        ObjectEntry, SegmentManifest, Sha256Digest, manifest_head, verify_manifest_signature,
    },
};

/// Byte cap on one manifest object.
///
/// A manifest describes a single segment copy, so a real one is a few
/// kilobytes. The cap is what stops a hostile archive from making the verifier
/// buffer an arbitrary object: [`read_capped`] issues a `HEAD` first and
/// refuses an oversized object before it reads a byte of it.
pub const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

/// Public keys the verifier accepts, keyed by `key_id`.
///
/// A manifest names the key that signed it. The verifier checks the signature
/// against the key this set holds under that name and never against the key the
/// manifest carries, because an attacker who can rewrite a manifest can rewrite
/// the key beside it.
#[derive(Debug, Default)]
pub struct TrustedManifestKeys {
    keys: HashMap<String, Vec<u8>>,
}

impl TrustedManifestKeys {
    /// A set that trusts one raw Ed25519 public key under `key_id`.
    #[must_use]
    pub fn single(key_id: String, public_key: Vec<u8>) -> Self {
        Self {
            keys: HashMap::from([(key_id, public_key)]),
        }
    }

    /// The raw Ed25519 public key registered under `key_id`.
    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&[u8]> {
        self.keys.get(key_id).map(Vec::as_slice)
    }

    /// `true` when no key is trusted.
    ///
    /// A run against an empty set still recomputes the chain and checks every
    /// object, and it counts every signed manifest as untrusted. It proves
    /// internal consistency and says nothing about who wrote the archive.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// How hard the verifier works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerifyDepth {
    /// Read manifests, recompute the chain, check signatures, and `HEAD` every
    /// referenced object for existence and size. No segment body is downloaded.
    #[default]
    Shallow,
    /// Additionally download every object and recompute its `SHA-256`. The only
    /// check that catches a same-size body substitution.
    Deep,
}

/// What to verify, and against what expectation.
#[derive(Debug, Clone, Default)]
pub struct VerifyRequest {
    /// Key prefix inside the store. `None` verifies the whole store.
    pub prefix: Option<String>,
    /// Verify only the partitions of this topic.
    pub topic: Option<String>,
    /// Verify only this partition index.
    pub partition: Option<i32>,
    /// How much of each object to read.
    pub depth: VerifyDepth,
    /// Expected head of the newest manifest, obtained independently of the
    /// archive. Tail truncation leaves a shorter but internally perfect chain;
    /// nothing inside the archive detects it.
    ///
    /// A partition whose tip differs from this head is reported as a break, so
    /// a programmatic caller gets one `ok` to test. An archive with **no**
    /// partition left to check is the one case this cannot report, because the
    /// report has no partition to carry the break: check
    /// [`ArchiveVerifyReport::partitions`] for emptiness as well. The
    /// `crabka-worm-verify` binary leaves this `None` and compares the tips
    /// itself, because it grades a tip mismatch as its own outcome and not as
    /// tampering, and because it must also catch the emptied archive.
    pub expect_head: Option<ChainHead>,
    /// Treat an epoch restart as accepted rather than an attestation hole.
    ///
    /// Read by whoever grades the report. The chain walk always records every
    /// epoch it finds, and an epoch restart on its own never clears
    /// [`PartitionVerifyReport::ok`]: a restart is a hole in the attestation,
    /// not evidence of a rewrite.
    pub allow_epoch_restarts: bool,
}

/// One unbroken run of a partition's chain, as the archive holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochSpan {
    /// The run's identifier.
    pub epoch_id: EpochId,
    /// Sequence of the run's first manifest.
    pub first_seq: ManifestSeq,
    /// Sequence of the run's last verified manifest.
    pub last_seq: ManifestSeq,
    /// Manifests verified in this run.
    pub manifests: u64,
    /// Lowest segment start offset in the run.
    pub start_offset: i64,
    /// Highest segment end offset in the run.
    pub end_offset: i64,
    /// Chain head after the run's last verified manifest.
    pub head: ChainHead,
}

/// A hole between two consecutive archived segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetGap {
    /// Last offset the archive holds before the hole.
    pub after: i64,
    /// First offset the archive holds after the hole.
    pub before: i64,
}

/// The first detected break in one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyBreak {
    /// Object-store key of the manifest the walk stopped at.
    pub manifest_key: String,
    /// Chain position of that manifest, when it decoded far enough to have one.
    pub seq: Option<ManifestSeq>,
    /// What is wrong, in one sentence.
    pub reason: String,
}

/// Verification result for one partition directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionVerifyReport {
    /// The directory this report covers, as an object-store key prefix.
    pub partition_dir: String,
    /// Manifests verified before the walk stopped.
    pub manifests: u64,
    /// Object entries checked before the walk stopped.
    pub objects_checked: u64,
    /// Chain runs found, ordered by their lowest segment start offset.
    pub epochs: Vec<EpochSpan>,
    /// Manifests that carry no signature at all.
    pub unsigned_manifests: u64,
    /// Manifests signed by a `key_id` the run does not trust.
    pub untrusted_manifests: u64,
    /// Objects in the directory that no manifest names, sorted.
    pub orphan_objects: Vec<String>,
    /// Holes between consecutive archived segments, sorted.
    pub offset_gaps: Vec<OffsetGap>,
    /// Chain head after the last verified manifest.
    pub head: Option<ChainHead>,
    /// `false` when the walk found a break.
    pub ok: bool,
    /// The break, when there is one.
    pub first_break: Option<VerifyBreak>,
}

/// Verification result for a whole archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveVerifyReport {
    /// One report per partition directory, sorted by directory.
    pub partitions: Vec<PartitionVerifyReport>,
}

impl ArchiveVerifyReport {
    /// `true` when no partition broke.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.partitions.iter().all(|partition| partition.ok)
    }

    /// Manifests verified across every partition.
    #[must_use]
    pub fn manifests(&self) -> u64 {
        self.partitions.iter().fold(0u64, |total, partition| {
            total.saturating_add(partition.manifests)
        })
    }

    /// The break in the first broken partition, in partition order.
    #[must_use]
    pub fn first_break(&self) -> Option<&VerifyBreak> {
        self.partitions
            .iter()
            .find_map(|partition| partition.first_break.as_ref())
    }

    /// Every manifest verified against a trusted key.
    #[must_use]
    pub fn fully_attested(&self) -> bool {
        self.partitions.iter().all(|partition| {
            partition.unsigned_manifests == 0 && partition.untrusted_manifests == 0
        })
    }

    /// Any partition holds more than one chain run.
    #[must_use]
    pub fn has_epoch_restarts(&self) -> bool {
        self.partitions
            .iter()
            .any(|partition| partition.epochs.len() > 1)
    }
}

/// Verifies every partition the request selects.
///
/// The walk stops at the first break **per partition** and keeps going with the
/// other partitions, so one damaged partition does not hide the state of the
/// rest. It does no recovery and no truncation, so tail damage stays visible.
/// The report is deterministic: two runs against an unchanged archive produce
/// equal values.
///
/// # Errors
///
/// [`WormError`] **only** when the archive cannot be listed, a manifest object
/// cannot be read, or — under [`VerifyDepth::Deep`] — a listed object body
/// cannot be fetched. A *tampered* archive is a successful call with `ok ==
/// false`: "I could not look" and "I looked and it is broken" are different
/// outcomes, and the exit-code grading depends on the difference.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        prefix = request.prefix.as_deref().unwrap_or(""),
        depth = ?request.depth,
    )
)]
pub async fn verify_archive(
    store: &Arc<dyn ObjectStore>,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<ArchiveVerifyReport, WormError> {
    let listing = list_archive(store, request.prefix.as_deref()).await?;
    let mut partitions = Vec::new();
    for (dir, entries) in &listing {
        if let Some(report) = verify_partition(store, dir, entries, request, trusted).await? {
            partitions.push(report);
        }
    }
    // The listing is a `BTreeMap` keyed by directory, so the partitions are
    // already in directory order. Sorting again states the guarantee locally.
    partitions.sort_by(|a, b| a.partition_dir.cmp(&b.partition_dir));
    Ok(ArchiveVerifyReport { partitions })
}

/// One directory's objects: full object-store key to size in bytes.
type DirListing = BTreeMap<String, u64>;

/// Lists the archive and groups it by the directory each key sits in.
async fn list_archive(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&str>,
) -> Result<BTreeMap<String, DirListing>, WormError> {
    let root = prefix.map(Path::from);
    let mut stream = store.list(root.as_ref());
    let mut dirs: BTreeMap<String, DirListing> = BTreeMap::new();
    loop {
        let next = stream.try_next().await.map_err(|e| {
            WormError::Archive(format!(
                "cannot list the archive under `{}`: {e}",
                prefix.unwrap_or("")
            ))
        })?;
        let Some(meta) = next else { break };
        let key = meta.location.to_string();
        let dir = key
            .rsplit_once('/')
            .map_or_else(String::new, |(dir, _)| dir.to_string());
        dirs.entry(dir).or_default().insert(key, meta.size);
    }
    Ok(dirs)
}

/// One manifest object, as far as the verifier could take it.
enum ManifestRead {
    /// The object decoded into a manifest of a supported format version.
    Decoded(Box<SegmentManifest>),
    /// The object is not a manifest this verifier accepts, and why.
    Rejected(String),
}

/// Reads and decodes one manifest object.
async fn read_manifest(store: &Arc<dyn ObjectStore>, key: &str) -> Result<ManifestRead, WormError> {
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
    if manifest.body.format_version != MANIFEST_FORMAT_VERSION {
        return Ok(ManifestRead::Rejected(format!(
            "manifest format version {} is not the supported version {MANIFEST_FORMAT_VERSION}",
            manifest.body.format_version
        )));
    }
    Ok(ManifestRead::Decoded(Box::new(manifest)))
}

/// One manifest object and the key it was read from.
type KeyedManifest = (String, SegmentManifest);

/// Verifies one partition directory, or `None` when the request filters it out.
async fn verify_partition(
    store: &Arc<dyn ObjectStore>,
    dir: &str,
    listing: &DirListing,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<Option<PartitionVerifyReport>, WormError> {
    let mut decoded: Vec<KeyedManifest> = Vec::new();
    let mut rejected: Option<VerifyBreak> = None;
    for key in listing.keys().filter(|key| key.ends_with(MANIFEST_SUFFIX)) {
        match read_manifest(store, key).await? {
            ManifestRead::Decoded(manifest) => decoded.push((key.clone(), *manifest)),
            ManifestRead::Rejected(reason) => {
                rejected.get_or_insert_with(|| VerifyBreak {
                    manifest_key: key.clone(),
                    seq: None,
                    reason,
                });
            }
        }
    }

    if !selected(request, decoded.first().map(|(_, m)| &m.body)) {
        return Ok(None);
    }

    let orphan_objects = orphans(listing, &decoded);
    if let Some(first_break) = rejected {
        return Ok(Some(broken_before_walk(dir, orphan_objects, first_break)));
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

/// Objects in the directory that no manifest names, sorted by key.
fn orphans(listing: &DirListing, decoded: &[KeyedManifest]) -> Vec<String> {
    let referenced: BTreeSet<&str> = decoded
        .iter()
        .flat_map(|(_, manifest)| {
            manifest
                .body
                .objects
                .iter()
                .map(|object| object.key.as_str())
        })
        .collect();
    listing
        .keys()
        .filter(|key| !key.ends_with(MANIFEST_SUFFIX) && !referenced.contains(key.as_str()))
        .cloned()
        .collect()
}

/// A partition whose manifests could not all be decoded, so no walk ran.
fn broken_before_walk(
    dir: &str,
    orphan_objects: Vec<String>,
    first_break: VerifyBreak,
) -> PartitionVerifyReport {
    PartitionVerifyReport {
        partition_dir: dir.to_string(),
        manifests: 0,
        objects_checked: 0,
        epochs: Vec::new(),
        unsigned_manifests: 0,
        untrusted_manifests: 0,
        orphan_objects,
        offset_gaps: Vec::new(),
        head: None,
        ok: false,
        first_break: Some(first_break),
    }
}

/// Everything the chain walk accumulates for one partition.
#[derive(Default)]
struct Walk {
    manifests: u64,
    objects_checked: u64,
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
        self.segments
            .push((body.segment.start_offset, body.segment.end_offset));
        self.head = Some(head);
        self.last = Some((key.to_string(), body.chain.seq));
    }

    /// Turns the accumulated walk into the partition's report.
    fn into_report(
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
async fn walk_partition(
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

/// What one manifest's signature is worth to this run.
enum SignatureState {
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
fn signature_state(manifest: &SegmentManifest, trusted: &TrustedManifestKeys) -> SignatureState {
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

/// Checks every object a manifest names, to the requested depth.
///
/// An object must live in the manifest's own partition directory. A manifest
/// that points somewhere else names an object this walk cannot account for, and
/// it counts as missing.
async fn check_objects(
    store: &Arc<dyn ObjectStore>,
    manifest: &SegmentManifest,
    listing: &DirListing,
    depth: VerifyDepth,
) -> Result<Option<String>, WormError> {
    for object in &manifest.body.objects {
        let Some(&size) = listing.get(&object.key) else {
            return Ok(Some(format!(
                "object `{}` named by the manifest is missing from the archive",
                object.key
            )));
        };
        if size != object.size_bytes {
            return Ok(Some(format!(
                "object `{}` is {size} bytes, the manifest records a size of {} bytes",
                object.key, object.size_bytes
            )));
        }
        if depth == VerifyDepth::Deep {
            let digest = object_digest(store, &object.key, None).await?;
            if digest != object.sha256 {
                let pinned = pinned_version_note(store, object).await;
                return Ok(Some(format!(
                    "object `{}` hashes to {digest}, the manifest records {}{pinned}",
                    object.key, object.sha256
                )));
            }
        }
    }
    Ok(None)
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
        version: version.map(ToString::to_string),
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

/// Opens or widens the span that describes the run being walked.
fn extend_span(span: &mut Option<EpochSpan>, body: &ManifestBody, head: ChainHead) {
    match span {
        None => {
            *span = Some(EpochSpan {
                epoch_id: body.chain.epoch_id,
                first_seq: body.chain.seq,
                last_seq: body.chain.seq,
                manifests: 1,
                start_offset: body.segment.start_offset,
                end_offset: body.segment.end_offset,
                head,
            });
        }
        Some(span) => {
            span.last_seq = body.chain.seq;
            span.manifests = span.manifests.saturating_add(1);
            span.start_offset = span.start_offset.min(body.segment.start_offset);
            span.end_offset = span.end_offset.max(body.segment.end_offset);
            span.head = head;
        }
    }
}

/// Holes between the offset ranges the verified segments cover.
fn offset_gaps(segments: &mut [(i64, i64)]) -> Vec<OffsetGap> {
    segments.sort_unstable();
    let mut gaps = Vec::new();
    let mut covered: Option<i64> = None;
    for &(start, end) in &*segments {
        if let Some(after) = covered
            && start > after.saturating_add(1)
        {
            gaps.push(OffsetGap {
                after,
                before: start,
            });
        }
        covered = Some(covered.map_or(end, |after| after.max(end)));
    }
    gaps
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use bytes::Bytes;
    use crabka_audit::signing::FileEd25519Signer;
    use crabka_ids::LeaderEpoch;
    use crabka_object_store::{ObjectOps, ObjectStoreClient, PutRequest};
    use object_store::{ObjectStoreExt as _, memory::InMemory};
    use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata::{
            RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
            RemoteLogSegmentState, TopicIdPartition,
        },
        storage_manager::{partition_dir_name, segment_file_name},
        worm::{
            archiver::WormArchiver,
            chain::WormChainRecord,
            manifest::{ChainStamp, ObjectEntry},
        },
    };

    const TOPIC: &str = "orders";
    const PARTITION: i32 = 0;
    const KEY_ID: &str = "worm-key-1";
    const PREFIX: &str = "archive";
    const STRAY: &str = "stray.bin";
    /// Offsets one fixture segment covers.
    const SEGMENT_SPAN: i64 = 100;

    /// A throwaway Ed25519 signer, and the raw public key that verifies it.
    fn signer(key_id: &str) -> (Arc<FileEd25519Signer>, Vec<u8>) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_string())
            .expect("ring mints a valid PKCS#8 Ed25519 key");
        let public_key = signer.public_key();
        (Arc::new(signer), public_key)
    }

    fn metadata(index: usize) -> RemoteLogSegmentMetadata {
        let start = i64::try_from(index).unwrap() * SEGMENT_SPAN;
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), TOPIC, PARTITION),
                Uuid::from_u128(0x1000 + u128::try_from(index).unwrap()),
            ),
            start,
            start + SEGMENT_SPAN - 1,
            1_713_000_000_000,
            1,
            1_713_000_001_000,
            RemoteLogSegmentDetails::new(
                4096,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), start)]),
            ),
        )
        .unwrap()
    }

    /// One archived segment, and what the fixture needs to tamper with it.
    struct Segment {
        metadata: RemoteLogSegmentMetadata,
        manifest_key: String,
        log_key: String,
        log_body: Vec<u8>,
        entries: Vec<ObjectEntry>,
        manifest: SegmentManifest,
    }

    /// A WORM archive built object by object, the way a backend writes one.
    struct Archive {
        store: Arc<dyn ObjectStore>,
        ops: ObjectStoreClient,
        dir: String,
        segments: Vec<Segment>,
        public_key: Vec<u8>,
        signer: Arc<FileEd25519Signer>,
    }

    impl Archive {
        /// Builds an archive whose chain runs hold `runs[i]` segments each.
        ///
        /// The objects go in through raw [`ObjectOps`] puts and the manifests
        /// through [`WormArchiver`], so the fixture never borrows the backend's
        /// idea of what a correct archive looks like.
        async fn build(runs: &[usize]) -> Self {
            Self::build_on(Arc::new(InMemory::new()), runs).await
        }

        /// The same archive, on a store the caller chose.
        async fn build_on(store: Arc<dyn ObjectStore>, runs: &[usize]) -> Self {
            let ops = ObjectStoreClient::new(Arc::clone(&store));
            let (signer, public_key) = signer(KEY_ID);
            let archiver = WormArchiver::new(Some(Arc::clone(&signer)));
            let dir = format!("{PREFIX}/{}", partition_dir_name(&metadata(0)));

            let mut segments = Vec::new();
            let mut index = 0usize;
            for run in runs {
                let epoch = EpochId(Uuid::new_v4());
                let mut prev_head = ChainHead::GENESIS;
                for seq in 0..*run {
                    let md = metadata(index);
                    let log_body = format!("segment-{index}-log-body").into_bytes();
                    let index_body = format!("segment-{index}-index").into_bytes();
                    let log_key = format!("{dir}/{}", segment_file_name(&md, ".log"));
                    let index_key = format!("{dir}/{}", segment_file_name(&md, ".index"));
                    let entries = vec![
                        put_entry(&ops, ".log", &log_key, &log_body).await,
                        put_entry(&ops, ".index", &index_key, &index_body).await,
                    ];
                    let stamped = md.clone().with_custom_metadata(
                        WormChainRecord::request(ChainStamp {
                            epoch_id: epoch,
                            seq: ManifestSeq(u64::try_from(seq).unwrap()),
                            prev_head,
                        })
                        .to_custom_metadata(),
                    );
                    let sealed = archiver.seal(&stamped, entries.clone()).unwrap();
                    let manifest_key = format!("{dir}/{}", segment_file_name(&md, MANIFEST_SUFFIX));
                    put_raw(&ops, &manifest_key, sealed.bytes.clone()).await;
                    prev_head = manifest_head(&sealed.manifest.body);
                    segments.push(Segment {
                        metadata: md,
                        manifest_key,
                        log_key,
                        log_body,
                        entries,
                        manifest: sealed.manifest,
                    });
                    index += 1;
                }
            }
            Self {
                store,
                ops,
                dir,
                segments,
                public_key,
                signer,
            }
        }

        fn trusted(&self) -> TrustedManifestKeys {
            TrustedManifestKeys::single(KEY_ID.to_string(), self.public_key.clone())
        }

        /// Chain head of the newest manifest, before any tampering.
        fn tip(&self) -> ChainHead {
            manifest_head(
                &self
                    .segments
                    .last()
                    .expect("the fixture always builds at least one segment")
                    .manifest
                    .body,
            )
        }

        /// Re-seals one manifest in place, so the fixture can change the key
        /// that signed it or the head it claims to follow.
        async fn reseal(
            &self,
            index: usize,
            signer: Option<Arc<FileEd25519Signer>>,
            prev_head: Option<ChainHead>,
        ) {
            let segment = &self.segments[index];
            let mut stamp = segment.manifest.body.chain;
            if let Some(head) = prev_head {
                stamp.prev_head = head;
            }
            let stamped = segment
                .metadata
                .clone()
                .with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata());
            let sealed = WormArchiver::new(signer)
                .seal(&stamped, segment.entries.clone())
                .unwrap();
            put_raw(&self.ops, &segment.manifest_key, sealed.bytes).await;
        }

        async fn delete(&self, key: &str) {
            self.ops.delete(&Path::from(key)).await.unwrap();
        }
    }

    /// An in-memory store that actually keeps versions.
    ///
    /// [`InMemory`] reports `version: None` on every put and ignores a version
    /// asked for on get, so a test built on it would be asserting that fake's
    /// indifference rather than the verifier's handling of a versioned bucket.
    /// This keeps each put's bytes to one side and serves them back when a get
    /// pins that version, which is what an Object Lock bucket does and the
    /// whole reason [`pinned_version_note`] exists.
    ///
    /// The versions live outside the inner store, so they never appear in a
    /// listing: the verifier walks the same archive it would on S3, and the
    /// history is reachable only by asking for it.
    #[derive(Debug)]
    struct VersionedStore {
        inner: InMemory,
        history: std::sync::Mutex<HashMap<(String, String), Bytes>>,
        next: std::sync::atomic::AtomicU64,
    }

    impl VersionedStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                history: std::sync::Mutex::new(HashMap::new()),
                next: std::sync::atomic::AtomicU64::new(0),
            }
        }
    }

    impl std::fmt::Display for VersionedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "VersionedStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for VersionedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let version = format!(
                "v{}",
                self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            );
            let bytes = Bytes::from(payload.iter().flatten().copied().collect::<Vec<u8>>());
            let mut result = self.inner.put_opts(location, payload, opts).await?;
            self.history
                .lock()
                .expect("no test panics while holding this")
                .insert((location.to_string(), version.clone()), bytes);
            result.version = Some(version);
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let Some(version) = options.version.clone() else {
                return self.inner.get_opts(location, options).await;
            };
            let bytes = self
                .history
                .lock()
                .expect("no test panics while holding this")
                .get(&(location.to_string(), version.clone()))
                .cloned();
            let Some(bytes) = bytes else {
                return Err(object_store::Error::NotFound {
                    path: format!("{location}?versionId={version}"),
                    source: "no such version".into(),
                });
            };
            let mut meta = self.inner.head(location).await?;
            meta.size = bytes.len() as u64;
            meta.version = Some(version);
            Ok(object_store::GetResult {
                range: 0..meta.size,
                payload: object_store::GetResultPayload::Stream(Box::pin(
                    futures_util::stream::once(async move { Ok(bytes) }),
                )),
                meta,
                attributes: object_store::Attributes::default(),
            })
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Puts one object and returns the manifest entry that records it.
    async fn put_entry(
        ops: &ObjectStoreClient,
        suffix: &str,
        key: &str,
        body: &[u8],
    ) -> ObjectEntry {
        let outcome = ops
            .put(
                &Path::from(key),
                Bytes::copy_from_slice(body),
                PutRequest {
                    digest: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ObjectEntry {
            suffix: suffix.to_string(),
            key: key.to_string(),
            size_bytes: outcome.size_bytes,
            sha256: Sha256Digest(outcome.sha256.expect("the put was asked for a digest")),
            e_tag: outcome.e_tag,
            version_id: outcome.version_id,
        }
    }

    async fn put_raw(ops: &ObjectStoreClient, key: &str, body: Bytes) {
        ops.put(&Path::from(key), body, PutRequest::default())
            .await
            .unwrap();
    }

    /// One edit an attacker with write access to the bucket could make.
    #[derive(Clone, Copy)]
    enum Tamper {
        None,
        FlipLogByte(usize),
        TruncateLog(usize),
        DeleteLog(usize),
        RewritePrevHead(usize),
        DeleteManifest(usize),
        SignWithAnotherKey(usize),
        SignWithUnknownKeyId(usize),
        Unsign(usize),
        StrayObject,
        BumpFormatVersion(usize),
    }

    impl Tamper {
        async fn apply(self, archive: &Archive) {
            match self {
                Tamper::None => {}
                Tamper::FlipLogByte(i) => {
                    let segment = &archive.segments[i];
                    let mut body = segment.log_body.clone();
                    body[0] ^= 0xff;
                    put_raw(&archive.ops, &segment.log_key, Bytes::from(body)).await;
                }
                Tamper::TruncateLog(i) => {
                    let segment = &archive.segments[i];
                    let body = segment.log_body[..segment.log_body.len() - 1].to_vec();
                    put_raw(&archive.ops, &segment.log_key, Bytes::from(body)).await;
                }
                Tamper::DeleteLog(i) => archive.delete(&archive.segments[i].log_key).await,
                Tamper::RewritePrevHead(i) => {
                    archive
                        .reseal(
                            i,
                            Some(Arc::clone(&archive.signer)),
                            Some(ChainHead([0xaa; 32])),
                        )
                        .await;
                }
                Tamper::DeleteManifest(i) => {
                    archive.delete(&archive.segments[i].manifest_key).await;
                }
                Tamper::SignWithAnotherKey(i) => {
                    // Same `key_id`, different key material: the verifier must
                    // check against the key it trusts and not the key the
                    // manifest carries.
                    let (other, _) = signer(KEY_ID);
                    archive.reseal(i, Some(other), None).await;
                }
                Tamper::SignWithUnknownKeyId(i) => {
                    let (rogue, _) = signer("rogue-key");
                    archive.reseal(i, Some(rogue), None).await;
                }
                Tamper::Unsign(i) => archive.reseal(i, None, None).await,
                Tamper::StrayObject => {
                    put_raw(
                        &archive.ops,
                        &format!("{}/{STRAY}", archive.dir),
                        Bytes::from_static(b"nothing names me"),
                    )
                    .await;
                }
                Tamper::BumpFormatVersion(i) => {
                    let segment = &archive.segments[i];
                    let mut value: serde_json::Value =
                        serde_json::to_value(&segment.manifest).unwrap();
                    value["body"]["format_version"] =
                        serde_json::Value::from(MANIFEST_FORMAT_VERSION + 1);
                    let bytes = serde_json::to_vec(&value).unwrap();
                    put_raw(&archive.ops, &segment.manifest_key, Bytes::from(bytes)).await;
                }
            }
        }
    }

    /// The kind of break a row expects, matched against the reason text the
    /// report carries.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Category {
        Size,
        Missing,
        Chain,
        Signature,
        Format,
        Digest,
        Tip,
    }

    impl Category {
        fn matches(self, reason: &str) -> bool {
            match self {
                Category::Size => reason.contains("the manifest records a size of"),
                Category::Missing => reason.contains("is missing from the archive"),
                Category::Chain => {
                    reason.contains("chain sequence gap") || reason.contains("chain head mismatch")
                }
                Category::Signature => reason.contains("signature does not verify"),
                Category::Format => reason.contains("format version"),
                Category::Digest => reason.contains("hashes to"),
                Category::Tip => reason.contains("does not match the expected head"),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        Break(Category),
    }

    /// Which objects the row expects to find unaccounted for.
    #[derive(Clone, Copy)]
    enum Orphans {
        None,
        Stray,
        /// Every object of one segment, which is what deleting its manifest
        /// leaves behind.
        Segment(usize),
    }

    impl Orphans {
        fn expected(self, archive: &Archive) -> Vec<String> {
            match self {
                Orphans::None => Vec::new(),
                Orphans::Stray => vec![format!("{}/{STRAY}", archive.dir)],
                Orphans::Segment(i) => {
                    let mut keys: Vec<String> = archive.segments[i]
                        .entries
                        .iter()
                        .map(|entry| entry.key.clone())
                        .collect();
                    keys.sort();
                    keys
                }
            }
        }
    }

    struct Row {
        name: &'static str,
        runs: &'static [usize],
        tamper: Tamper,
        /// Pass the untampered tip as [`VerifyRequest::expect_head`].
        expect_tip: bool,
        shallow: Outcome,
        deep: Outcome,
        unsigned: u64,
        untrusted: u64,
        /// Chain runs the report must describe, when the walk completed.
        epoch_spans: Option<usize>,
        orphans: Orphans,
    }

    const fn row(name: &'static str, tamper: Tamper, shallow: Outcome, deep: Outcome) -> Row {
        Row {
            name,
            runs: &[3],
            tamper,
            expect_tip: false,
            shallow,
            deep,
            unsigned: 0,
            untrusted: 0,
            epoch_spans: None,
            orphans: Orphans::None,
        }
    }

    fn tamper_matrix() -> Vec<Row> {
        vec![
            Row {
                epoch_spans: Some(1),
                ..row("clean", Tamper::None, Outcome::Ok, Outcome::Ok)
            },
            // The one row that proves `--deep` earns its keep: the body changed
            // but the size did not, so nothing short of a re-hash sees it.
            row(
                "flip one byte in a .log",
                Tamper::FlipLogByte(1),
                Outcome::Ok,
                Outcome::Break(Category::Digest),
            ),
            row(
                "truncate a .log",
                Tamper::TruncateLog(1),
                Outcome::Break(Category::Size),
                Outcome::Break(Category::Size),
            ),
            row(
                "delete a .log",
                Tamper::DeleteLog(1),
                Outcome::Break(Category::Missing),
                Outcome::Break(Category::Missing),
            ),
            row(
                "manifest re-signed with a wrong prev_head",
                Tamper::RewritePrevHead(1),
                Outcome::Break(Category::Chain),
                Outcome::Break(Category::Chain),
            ),
            Row {
                orphans: Orphans::Segment(2),
                ..row(
                    "delete the newest manifest",
                    Tamper::DeleteManifest(2),
                    Outcome::Ok,
                    Outcome::Ok,
                )
            },
            Row {
                expect_tip: true,
                orphans: Orphans::Segment(2),
                ..row(
                    "delete the newest manifest, with an expected head",
                    Tamper::DeleteManifest(2),
                    Outcome::Break(Category::Tip),
                    Outcome::Break(Category::Tip),
                )
            },
            Row {
                orphans: Orphans::Segment(1),
                ..row(
                    "delete a middle manifest",
                    Tamper::DeleteManifest(1),
                    Outcome::Break(Category::Chain),
                    Outcome::Break(Category::Chain),
                )
            },
            row(
                "manifest signed by a different key",
                Tamper::SignWithAnotherKey(1),
                Outcome::Break(Category::Signature),
                Outcome::Break(Category::Signature),
            ),
            Row {
                untrusted: 1,
                epoch_spans: Some(1),
                ..row(
                    "unknown key_id",
                    Tamper::SignWithUnknownKeyId(1),
                    Outcome::Ok,
                    Outcome::Ok,
                )
            },
            Row {
                unsigned: 1,
                epoch_spans: Some(1),
                ..row(
                    "unsigned manifest",
                    Tamper::Unsign(1),
                    Outcome::Ok,
                    Outcome::Ok,
                )
            },
            Row {
                runs: &[2, 2],
                epoch_spans: Some(2),
                ..row("two epochs", Tamper::None, Outcome::Ok, Outcome::Ok)
            },
            Row {
                epoch_spans: Some(1),
                orphans: Orphans::Stray,
                ..row(
                    "stray object under the prefix",
                    Tamper::StrayObject,
                    Outcome::Ok,
                    Outcome::Ok,
                )
            },
            Row {
                // A manifest the verifier will not accept names nothing, so
                // the objects it used to account for become orphans.
                orphans: Orphans::Segment(1),
                ..row(
                    "format_version bumped",
                    Tamper::BumpFormatVersion(1),
                    Outcome::Break(Category::Format),
                    Outcome::Break(Category::Format),
                )
            },
        ]
    }

    async fn run_row(row: &Row, depth: VerifyDepth, expected: Outcome) {
        let archive = Archive::build(row.runs).await;
        let tip = archive.tip();
        row.tamper.apply(&archive).await;

        let request = VerifyRequest {
            depth,
            expect_head: row.expect_tip.then_some(tip),
            ..Default::default()
        };
        let report = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();
        let label = format!("{} / {depth:?}", row.name);
        check!(report.partitions.len() == 1, "{label}: one partition");
        let partition = &report.partitions[0];

        match expected {
            Outcome::Ok => {
                check!(
                    partition.first_break.as_ref().map(|b| b.reason.as_str()) == None,
                    "{label}"
                );
                check!(partition.ok, "{label}");
            }
            Outcome::Break(category) => {
                check!(!partition.ok, "{label}");
                match partition.first_break.as_ref() {
                    Some(found) => {
                        check!(
                            category.matches(&found.reason),
                            "{label}: `{}` is not a {category:?} break",
                            found.reason
                        );
                    }
                    None => {
                        check!(false, "{label}: ok is false with no break recorded");
                    }
                }
            }
        }
        check!(partition.unsigned_manifests == row.unsigned, "{label}");
        check!(partition.untrusted_manifests == row.untrusted, "{label}");
        check!(
            partition.orphan_objects == row.orphans.expected(&archive),
            "{label}"
        );
        if let Some(spans) = row.epoch_spans {
            check!(partition.epochs.len() == spans, "{label}");
        }
    }

    #[tokio::test]
    async fn shallow_verify_grades_every_tamper() {
        for row in tamper_matrix() {
            run_row(&row, VerifyDepth::Shallow, row.shallow).await;
        }
    }

    #[tokio::test]
    async fn deep_verify_grades_every_tamper() {
        for row in tamper_matrix() {
            run_row(&row, VerifyDepth::Deep, row.deep).await;
        }
    }

    #[tokio::test]
    async fn verify_reports_a_clean_archive_in_full() {
        let archive = Archive::build(&[3]).await;
        let report = verify_archive(
            &archive.store,
            &VerifyRequest::default(),
            &archive.trusted(),
        )
        .await
        .unwrap();

        let last = &archive.segments[2].manifest.body;
        let expected = ArchiveVerifyReport {
            partitions: vec![PartitionVerifyReport {
                partition_dir: archive.dir.clone(),
                manifests: 3,
                objects_checked: 6,
                epochs: vec![EpochSpan {
                    epoch_id: last.chain.epoch_id,
                    first_seq: ManifestSeq(0),
                    last_seq: ManifestSeq(2),
                    manifests: 3,
                    start_offset: 0,
                    end_offset: 3 * SEGMENT_SPAN - 1,
                    head: archive.tip(),
                }],
                unsigned_manifests: 0,
                untrusted_manifests: 0,
                orphan_objects: Vec::new(),
                offset_gaps: Vec::new(),
                head: Some(archive.tip()),
                ok: true,
                first_break: None,
            }],
        };
        check!(report == expected);
        check!(report.ok());
        check!(report.manifests() == 3);
        check!(report.fully_attested());
        check!(!report.has_epoch_restarts());
        check!(report.first_break() == None);
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

    #[tokio::test]
    async fn verify_of_an_empty_archive_is_ok() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let report = verify_archive(
            &store,
            &VerifyRequest::default(),
            &TrustedManifestKeys::default(),
        )
        .await
        .unwrap();

        check!(
            report
                == ArchiveVerifyReport {
                    partitions: Vec::new()
                }
        );
        check!(report.ok());
        check!(report.manifests() == 0);
        check!(report.fully_attested());
        check!(!report.has_epoch_restarts());
    }

    #[tokio::test]
    async fn a_topic_filter_skips_the_partitions_it_does_not_name() {
        let archive = Archive::build(&[2]).await;
        let trusted = archive.trusted();

        let other_topic = VerifyRequest {
            topic: Some("payments".to_string()),
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
            topic: Some(TOPIC.to_string()),
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

    #[tokio::test]
    async fn a_hole_between_segments_is_reported_as_an_offset_gap() {
        // Two runs, and the fixture numbers the second run's segments after a
        // deleted middle segment would sit, so the archive covers 0..99 and
        // 200..399 with nothing between.
        let archive = Archive::build(&[3]).await;
        archive.delete(&archive.segments[1].manifest_key).await;
        for entry in &archive.segments[1].entries {
            archive.delete(&entry.key).await;
        }
        // Re-chain the survivors so only the offsets are wrong: seq 2 now
        // follows seq 0, so re-stamp it at seq 1 on the first manifest's head.
        let head = manifest_head(&archive.segments[0].manifest.body);
        let segment = &archive.segments[2];
        let stamped = segment.metadata.clone().with_custom_metadata(
            WormChainRecord::request(ChainStamp {
                epoch_id: segment.manifest.body.chain.epoch_id,
                seq: ManifestSeq(1),
                prev_head: head,
            })
            .to_custom_metadata(),
        );
        let sealed = WormArchiver::new(Some(Arc::clone(&archive.signer)))
            .seal(&stamped, segment.entries.clone())
            .unwrap();
        put_raw(&archive.ops, &segment.manifest_key, sealed.bytes).await;

        let report = verify_archive(
            &archive.store,
            &VerifyRequest::default(),
            &archive.trusted(),
        )
        .await
        .unwrap();

        check!(report.ok());
        check!(
            report.partitions[0].offset_gaps
                == vec![OffsetGap {
                    after: SEGMENT_SPAN - 1,
                    before: 2 * SEGMENT_SPAN,
                }]
        );
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
        let swapped: Vec<u8> = segment.log_body.iter().map(|b| b ^ 0xff).collect();
        check!(swapped.len() == segment.log_body.len());
        put_raw(&archive.ops, &segment.log_key, Bytes::from(swapped)).await;

        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..VerifyRequest::default()
        };
        let report = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();

        check!(!report.ok(), "an overwritten body is a break");
        let reason = report
            .first_break()
            .map(|found| found.reason.clone())
            .unwrap_or_default();
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

        let swapped: Vec<u8> = segment.log_body.iter().map(|b| b ^ 0xff).collect();
        put_raw(&archive.ops, &segment.log_key, Bytes::from(swapped)).await;

        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..VerifyRequest::default()
        };
        let report = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();

        check!(!report.ok());
        let reason = report
            .first_break()
            .map(|found| found.reason.clone())
            .unwrap_or_default();
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
            version_id: Some("v-never-written".to_string()),
            ..entry
        };

        let note = pinned_version_note(&store, &missing).await;

        check!(note.contains("v-never-written"), "note was: {note}");
        check!(note.contains("could not be read"), "note was: {note}");
    }
}
