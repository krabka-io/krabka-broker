//! The segment manifest: the signed, chained record of one archived segment.
//!
//! A manifest names every object that one segment copy wrote, records a
//! `SHA-256` digest for each, and binds the whole set to the partition's hash
//! chain. [`canonical_manifest_bytes`] defines the byte encoding that the chain
//! hashes. The writer and the verifier both call it. Never reimplement the
//! layout, and never change it without changing
//! [`MANIFEST_FORMAT_VERSION`] with it.

use std::{collections::BTreeMap, fmt};

use crabka_audit::{
    chain::{GENESIS_HEAD, chain_hash, from_hex32, to_hex},
    signing::verify_signature,
};
use derive_more::{Display, From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::metadata::RemoteLogSegmentMetadata;

/// Version of the manifest encoding, both the JSON shape and the canonical
/// byte layout. A change to either is a change to this number.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Object-store key suffix of a segment manifest.
pub const MANIFEST_SUFFIX: &str = ".manifest";

/// Domain separation for the manifest signature. Distinct from
/// [`crabka_audit::signing::CHECKPOINT_DOMAIN`] — never share a domain
/// across signature purposes.
pub const MANIFEST_DOMAIN: &[u8] = b"crabka-worm-manifest-v1\0";

/// Domain separation for the chain preimage, so a manifest body can never
/// collide with any other chained value.
pub const MANIFEST_BODY_DOMAIN: &[u8] = b"crabka-worm-manifest-body-v1\0";

/// Position of one manifest in its partition's hash chain.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ManifestSeq(pub u64);

/// Identifier of one unbroken run of a partition's chain.
///
/// A chain that cannot find its previous head starts a new epoch rather than
/// silently restarting the old one at genesis.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct EpochId(pub Uuid);

/// Head of a partition's manifest hash chain.
///
/// Serialises as a lowercase hex string.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainHead(pub [u8; 32]);

impl ChainHead {
    /// The head before a chain writes its first manifest.
    pub const GENESIS: Self = Self(GENESIS_HEAD);
}

/// `SHA-256` digest of one archived object's body.
///
/// Serialises as a lowercase hex string.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256Digest(pub [u8; 32]);

impl Sha256Digest {
    /// Digest of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

/// Variable-length bytes that serialise as a lowercase hex string.
///
/// Used for the public key and the signature, neither of which has a fixed
/// length the type system can pin.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HexBytes(pub Vec<u8>);

fn serialize_hex<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&to_hex(bytes))
}

fn deserialize_hex32<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    from_hex32(&text)
        .ok_or_else(|| de::Error::custom(format!("expected 64 hex characters, got `{text}`")))
}

impl fmt::Display for ChainHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

impl fmt::Debug for ChainHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChainHead({})", to_hex(&self.0))
    }
}

impl Serialize for ChainHead {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_hex(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ChainHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_hex32(deserializer).map(Self)
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sha256Digest({})", to_hex(&self.0))
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_hex(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_hex32(deserializer).map(Self)
    }
}

impl fmt::Display for HexBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

impl fmt::Debug for HexBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HexBytes({})", to_hex(&self.0))
    }
}

impl Serialize for HexBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_hex(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for HexBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        hex::decode(&text).map(Self).map_err(de::Error::custom)
    }
}

/// One object that a segment copy wrote to the archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntry {
    /// File suffix this object carries, such as `.log` or `.index`.
    pub suffix: String,
    /// Full object-store key of the object.
    pub key: String,
    /// Size of the object body in bytes.
    pub size_bytes: u64,
    /// `SHA-256` of the object body. The only integrity claim in this entry.
    pub sha256: Sha256Digest,
    /// Object-store `ETag`, as the store reported it at write time.
    ///
    /// A **locator, not an integrity proof**. A multipart `ETag` is a checksum
    /// of the part checksums and not of the object body, so it cannot confirm
    /// the bytes. Use it to find and pin an object; use
    /// [`Self::sha256`] to prove it.
    pub e_tag: Option<String>,
    /// Object-store version id, when the bucket has versioning on.
    ///
    /// A **locator, not an integrity proof**. It names which version of the
    /// key to read back. [`Self::sha256`] is the only integrity claim.
    pub version_id: Option<String>,
}

/// The segment this manifest describes, flattened out of
/// [`RemoteLogSegmentMetadata`].
///
/// The manifest keeps its own copy rather than a reference to the metadata
/// store, because a verifier reads the archive alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentIdentity {
    /// Topic name, for diagnostics.
    pub topic: String,
    /// Stable topic id.
    pub topic_id: Uuid,
    /// Partition index.
    pub partition: i32,
    /// Per-segment id.
    pub segment_id: Uuid,
    /// First offset (inclusive) in the segment.
    pub start_offset: i64,
    /// Last offset (inclusive) in the segment.
    pub end_offset: i64,
    /// Highest record timestamp in the segment.
    pub max_timestamp_ms: i64,
    /// Broker that copied the segment.
    pub broker_id: i32,
    /// Wall-clock time of the copy event.
    pub event_timestamp_ms: i64,
    /// Size of the `.log` data in bytes.
    pub segment_size_bytes: i64,
    /// Leader epoch to the first offset that epoch contributed.
    pub leader_epochs: BTreeMap<i32, i64>,
    /// `true` when the segment has no transaction index.
    pub txn_index_empty: bool,
}

impl SegmentIdentity {
    /// Flattens the fields a manifest records out of `md`.
    #[must_use]
    pub fn from_metadata(md: &RemoteLogSegmentMetadata) -> Self {
        let id = md.remote_log_segment_id();
        Self {
            topic: id.topic_id_partition.topic.clone(),
            topic_id: id.topic_id_partition.topic_id,
            partition: id.topic_id_partition.partition,
            segment_id: id.id,
            start_offset: md.start_offset(),
            end_offset: md.end_offset(),
            max_timestamp_ms: md.max_timestamp_ms(),
            broker_id: md.broker_id(),
            event_timestamp_ms: md.event_timestamp_ms(),
            segment_size_bytes: i64::from(md.segment_size_in_bytes()),
            leader_epochs: md
                .segment_leader_epochs()
                .iter()
                .map(|(epoch, offset)| (epoch.0, *offset))
                .collect(),
            txn_index_empty: md.txn_index_empty(),
        }
    }
}

/// Where this manifest sits in its partition's hash chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainStamp {
    /// Chain run this manifest belongs to.
    pub epoch_id: EpochId,
    /// Position within the run, counted from zero.
    pub seq: ManifestSeq,
    /// Chain head as it was before this manifest.
    pub prev_head: ChainHead,
}

/// Ed25519 signature over one manifest's chain head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSignature {
    /// Identifier of the key that produced the signature.
    pub key_id: String,
    /// Raw Ed25519 public key of that key.
    pub public_key: HexBytes,
    /// Signature over [`manifest_signing_bytes`].
    pub signature: HexBytes,
}

/// Everything a manifest signature covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestBody {
    /// Encoding version. See [`MANIFEST_FORMAT_VERSION`].
    pub format_version: u32,
    /// The segment this manifest describes.
    pub segment: SegmentIdentity,
    /// Every object the copy wrote, in the order the copy wrote them.
    pub objects: Vec<ObjectEntry>,
    /// Chain position of this manifest.
    pub chain: ChainStamp,
}

/// One segment manifest, as it is written to and read back from the archive.
///
/// The signature is a **sibling** of the body and never a member of it. A
/// signature can then not be included in what it signs, and the property holds
/// by the shape of the type rather than by the care of the writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentManifest {
    /// The signed content.
    pub body: ManifestBody,
    /// The signature, when the archive has a signing key configured.
    pub signature: Option<ManifestSignature>,
}

/// Appends a big-endian `u64` length prefix.
///
/// `u64` rather than `u32` so the conversion from `usize` is lossless on every
/// target Crabka builds for. A saturating `u32` prefix would make the encoding
/// non-injective in principle — two different bodies could share a preimage —
/// and "a field that large cannot occur" is a claim about callers, not a
/// property of the encoding. A canonical encoding that a chain head depends on
/// should not rest on one, and four bytes per field is not worth the argument.
fn push_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&(len as u64).to_be_bytes());
}

/// Appends a length-prefixed byte string.
fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

/// Appends an optional string as a presence byte and, when present, a
/// length-prefixed body.
fn push_optional(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => out.push(0),
        Some(text) => {
            out.push(1);
            push_bytes(out, text.as_bytes());
        }
    }
}

/// Deterministic byte encoding of a manifest body.
///
/// This is the preimage the hash chain covers. The writer and the verifier
/// both call this function, and a disagreement between them makes every
/// archive written under the older encoding fail verification with no way to
/// tell tampering from a format change.
///
/// Every integer is big-endian, every length prefix is a `u64`, and every
/// string is `UTF-8`. Every variable-length field carries a length, so no two
/// distinct bodies encode to the same bytes.
///
/// ```text
/// MANIFEST_BODY_DOMAIN
/// format_version:u32
/// len+topic  topic_id:16  partition:i32  segment_id:16
/// start_offset:i64  end_offset:i64  max_timestamp_ms:i64
/// broker_id:i32  event_timestamp_ms:i64  segment_size_bytes:i64
/// leader_epochs.len():u64, then per entry in BTreeMap order: epoch:i32 offset:i64
/// txn_index_empty:u8
/// objects.len():u64, then per entry in vec order:
///     len+suffix  len+key  size_bytes:u64  sha256:32
///     e_tag:      0u8 | (1u8 len+value)
///     version_id: 0u8 | (1u8 len+value)
/// epoch_id:16  seq:u64  prev_head:32
/// ```
#[must_use]
pub fn canonical_manifest_bytes(body: &ManifestBody) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MANIFEST_BODY_DOMAIN);
    out.extend_from_slice(&body.format_version.to_be_bytes());

    let segment = &body.segment;
    push_bytes(&mut out, segment.topic.as_bytes());
    out.extend_from_slice(segment.topic_id.as_bytes());
    out.extend_from_slice(&segment.partition.to_be_bytes());
    out.extend_from_slice(segment.segment_id.as_bytes());
    out.extend_from_slice(&segment.start_offset.to_be_bytes());
    out.extend_from_slice(&segment.end_offset.to_be_bytes());
    out.extend_from_slice(&segment.max_timestamp_ms.to_be_bytes());
    out.extend_from_slice(&segment.broker_id.to_be_bytes());
    out.extend_from_slice(&segment.event_timestamp_ms.to_be_bytes());
    out.extend_from_slice(&segment.segment_size_bytes.to_be_bytes());
    push_len(&mut out, segment.leader_epochs.len());
    for (epoch, offset) in &segment.leader_epochs {
        out.extend_from_slice(&epoch.to_be_bytes());
        out.extend_from_slice(&offset.to_be_bytes());
    }
    out.push(u8::from(segment.txn_index_empty));

    push_len(&mut out, body.objects.len());
    for object in &body.objects {
        push_bytes(&mut out, object.suffix.as_bytes());
        push_bytes(&mut out, object.key.as_bytes());
        out.extend_from_slice(&object.size_bytes.to_be_bytes());
        out.extend_from_slice(&object.sha256.0);
        push_optional(&mut out, object.e_tag.as_deref());
        push_optional(&mut out, object.version_id.as_deref());
    }

    out.extend_from_slice(body.chain.epoch_id.0.as_bytes());
    out.extend_from_slice(&body.chain.seq.0.to_be_bytes());
    out.extend_from_slice(&body.chain.prev_head.0);
    out
}

/// Chain head after `body`.
///
/// Chains the canonical bytes of the body onto the head the body itself
/// records as its predecessor.
#[must_use]
pub fn manifest_head(body: &ManifestBody) -> ChainHead {
    ChainHead(chain_hash(
        &body.chain.prev_head.0,
        body.chain.seq.0,
        &canonical_manifest_bytes(body),
    ))
}

/// Canonical signed payload for a manifest.
///
/// The layout is
/// `MANIFEST_DOMAIN ‖ kid_len(u32 BE) ‖ kid ‖ epoch_id(16) ‖ seq(u64 BE) ‖ head(32)`.
/// The writer calls it to sign and the verifier calls it to verify.
#[must_use]
pub fn manifest_signing_bytes(
    key_id: &str,
    epoch_id: EpochId,
    seq: ManifestSeq,
    head: ChainHead,
) -> Vec<u8> {
    let kid = key_id.as_bytes();
    let mut out = Vec::with_capacity(MANIFEST_DOMAIN.len() + 4 + kid.len() + 16 + 8 + 32);
    out.extend_from_slice(MANIFEST_DOMAIN);
    push_bytes(&mut out, kid);
    out.extend_from_slice(epoch_id.0.as_bytes());
    out.extend_from_slice(&seq.0.to_be_bytes());
    out.extend_from_slice(&head.0);
    out
}

/// Recomputes the head from the body, then checks the signature against
/// `public_key`.
///
/// Returns `false` for an unsigned manifest. A caller that treats "unsigned"
/// and "bad signature" differently must test `manifest.signature.is_none()`
/// first.
#[must_use]
pub fn verify_manifest_signature(manifest: &SegmentManifest, public_key: &[u8]) -> bool {
    let Some(signature) = manifest.signature.as_ref() else {
        return false;
    };
    let head = manifest_head(&manifest.body);
    let message = manifest_signing_bytes(
        &signature.key_id,
        manifest.body.chain.epoch_id,
        manifest.body.chain.seq,
        head,
    );
    verify_signature(public_key, &message, &signature.signature.0)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_audit::{
        ids::{EpochMs, Seq},
        signing::{
            CHECKPOINT_DOMAIN, FileEd25519Signer, SigningKeyProvider, checkpoint_signing_bytes,
        },
    };
    use crabka_ids::LeaderEpoch;
    use ring::{rand::SystemRandom, signature::Ed25519KeyPair};

    use super::*;
    use crate::metadata::{
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition,
    };

    const KEY_ID: &str = "worm-key-1";

    /// One labelled edit to a manifest body, for the preimage-coverage table.
    type Mutation = (&'static str, Box<dyn Fn(&mut ManifestBody)>);

    fn located_object() -> ObjectEntry {
        ObjectEntry {
            suffix: ".log".to_string(),
            key: "archive/orders-3/00000000000000000100.log".to_string(),
            size_bytes: 4096,
            sha256: Sha256Digest::of(b"log body"),
            e_tag: Some("\"d41d8cd98f00b204e9800998ecf8427e-3\"".to_string()),
            version_id: Some("3HL4kqtJlcpXroDTDmjVBH40Nrjfkd".to_string()),
        }
    }

    fn bare_object() -> ObjectEntry {
        ObjectEntry {
            suffix: ".index".to_string(),
            key: "archive/orders-3/00000000000000000100.index".to_string(),
            size_bytes: 64,
            sha256: Sha256Digest::of(b"index body"),
            e_tag: None,
            version_id: None,
        }
    }

    fn sample_body() -> ManifestBody {
        ManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            segment: SegmentIdentity {
                topic: "orders".to_string(),
                topic_id: Uuid::from_u128(0x11),
                partition: 3,
                segment_id: Uuid::from_u128(0x22),
                start_offset: 100,
                end_offset: 199,
                max_timestamp_ms: 1_713_000_000_000,
                broker_id: 7,
                event_timestamp_ms: 1_713_000_001_000,
                segment_size_bytes: 4096,
                leader_epochs: BTreeMap::from([(0, 100), (1, 150)]),
                txn_index_empty: false,
            },
            objects: vec![located_object(), bare_object()],
            chain: ChainStamp {
                epoch_id: EpochId(Uuid::from_u128(0x99)),
                seq: ManifestSeq(4),
                prev_head: ChainHead([7u8; 32]),
            },
        }
    }

    fn fresh_signer(key_id: &str) -> (FileEd25519Signer, Vec<u8>) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer =
            FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_string()).unwrap();
        let public_key = signer.public_key();
        (signer, public_key)
    }

    fn sign_manifest(body: ManifestBody, signer: &FileEd25519Signer) -> SegmentManifest {
        let head = manifest_head(&body);
        let message =
            manifest_signing_bytes(signer.key_id(), body.chain.epoch_id, body.chain.seq, head);
        SegmentManifest {
            signature: Some(ManifestSignature {
                key_id: signer.key_id().to_string(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }),
            body,
        }
    }

    #[test]
    fn canonical_bytes_survive_json_reserialization() {
        let body = sample_body();
        let expected = canonical_manifest_bytes(&body);

        let compact = serde_json::to_string(&body).unwrap();
        let from_compact: ManifestBody = serde_json::from_str(&compact).unwrap();
        check!(
            canonical_manifest_bytes(&from_compact) == expected,
            "compact"
        );

        let pretty = serde_json::to_string_pretty(&from_compact).unwrap();
        let from_pretty: ManifestBody = serde_json::from_str(&pretty).unwrap();
        check!(canonical_manifest_bytes(&from_pretty) == expected, "pretty");

        // A second full cycle, so an encoding that is stable only on the first
        // pass does not slip through.
        let again: ManifestBody =
            serde_json::from_str(&serde_json::to_string(&from_pretty).unwrap()).unwrap();
        check!(canonical_manifest_bytes(&again) == expected, "second cycle");
        check!(again == body, "structural equality");
        check!(manifest_head(&again) == manifest_head(&body), "head");
    }

    #[test]
    fn canonical_bytes_change_with_every_field() {
        let base = sample_body();
        let base_bytes = canonical_manifest_bytes(&base);

        let mutations: Vec<Mutation> = vec![
            ("format_version", Box::new(|b| b.format_version += 1)),
            (
                "segment.topic",
                Box::new(|b| b.segment.topic = "payments".to_string()),
            ),
            (
                "segment.topic_id",
                Box::new(|b| b.segment.topic_id = Uuid::from_u128(0x12)),
            ),
            ("segment.partition", Box::new(|b| b.segment.partition = 4)),
            (
                "segment.segment_id",
                Box::new(|b| b.segment.segment_id = Uuid::from_u128(0x23)),
            ),
            (
                "segment.start_offset",
                Box::new(|b| b.segment.start_offset = 101),
            ),
            (
                "segment.end_offset",
                Box::new(|b| b.segment.end_offset = 200),
            ),
            (
                "segment.max_timestamp_ms",
                Box::new(|b| b.segment.max_timestamp_ms += 1),
            ),
            ("segment.broker_id", Box::new(|b| b.segment.broker_id = 8)),
            (
                "segment.event_timestamp_ms",
                Box::new(|b| b.segment.event_timestamp_ms += 1),
            ),
            (
                "segment.segment_size_bytes",
                Box::new(|b| b.segment.segment_size_bytes += 1),
            ),
            (
                "segment.leader_epochs value",
                Box::new(|b| {
                    b.segment.leader_epochs.insert(1, 151);
                }),
            ),
            (
                "segment.leader_epochs extra entry",
                Box::new(|b| {
                    b.segment.leader_epochs.insert(2, 180);
                }),
            ),
            (
                "segment.leader_epochs removed entry",
                Box::new(|b| {
                    b.segment.leader_epochs.remove(&1);
                }),
            ),
            (
                "segment.txn_index_empty",
                Box::new(|b| b.segment.txn_index_empty = true),
            ),
            (
                "objects[0].suffix",
                Box::new(|b| b.objects[0].suffix = ".timeindex".to_string()),
            ),
            (
                "objects[0].key",
                Box::new(|b| b.objects[0].key = "archive/elsewhere.log".to_string()),
            ),
            (
                "objects[0].size_bytes",
                Box::new(|b| b.objects[0].size_bytes += 1),
            ),
            (
                "objects[0].sha256",
                Box::new(|b| b.objects[0].sha256 = Sha256Digest::of(b"other body")),
            ),
            (
                "objects[0].e_tag changed",
                Box::new(|b| b.objects[0].e_tag = Some("\"other\"".to_string())),
            ),
            (
                "objects[0].e_tag cleared",
                Box::new(|b| b.objects[0].e_tag = None),
            ),
            (
                "objects[1].e_tag set",
                Box::new(|b| b.objects[1].e_tag = Some("\"new\"".to_string())),
            ),
            (
                "objects[0].version_id changed",
                Box::new(|b| b.objects[0].version_id = Some("other".to_string())),
            ),
            (
                "objects[0].version_id cleared",
                Box::new(|b| b.objects[0].version_id = None),
            ),
            (
                "objects[1].version_id set",
                Box::new(|b| b.objects[1].version_id = Some("new".to_string())),
            ),
            ("objects order", Box::new(|b| b.objects.swap(0, 1))),
            (
                "objects count grows",
                Box::new(|b| b.objects.push(bare_object())),
            ),
            (
                "objects count shrinks",
                Box::new(|b| {
                    b.objects.pop();
                }),
            ),
            (
                "chain.epoch_id",
                Box::new(|b| b.chain.epoch_id = EpochId(Uuid::from_u128(0x98))),
            ),
            ("chain.seq", Box::new(|b| b.chain.seq = ManifestSeq(5))),
            (
                "chain.prev_head",
                Box::new(|b| b.chain.prev_head = ChainHead([8u8; 32])),
            ),
        ];

        for (name, mutate) in mutations {
            let mut mutated = base.clone();
            mutate(&mut mutated);
            check!(mutated != base, "case {name} did not change the body");
            check!(
                canonical_manifest_bytes(&mutated) != base_bytes,
                "case {name} is missing from the preimage"
            );
            check!(
                manifest_head(&mutated) != manifest_head(&base),
                "case {name} does not move the chain head"
            );
        }
    }

    #[test]
    fn length_prefixes_prevent_field_boundary_ambiguity() {
        let mut left = sample_body();
        left.segment.topic = "ab".to_string();
        left.objects = vec![ObjectEntry {
            key: "c".to_string(),
            ..bare_object()
        }];

        let mut right = left.clone();
        right.segment.topic = "a".to_string();
        right.objects[0].key = "bc".to_string();

        check!(canonical_manifest_bytes(&left) != canonical_manifest_bytes(&right));
    }

    #[test]
    fn manifest_json_round_trips() {
        let every_optional_populated = SegmentManifest {
            body: ManifestBody {
                objects: vec![located_object()],
                ..sample_body()
            },
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(vec![0x01, 0x02, 0x03]),
                signature: HexBytes(vec![0xfe, 0xed]),
            }),
        };
        let every_optional_absent = SegmentManifest {
            body: ManifestBody {
                objects: vec![bare_object()],
                ..sample_body()
            },
            signature: None,
        };

        for (name, manifest) in [
            ("every optional populated", every_optional_populated),
            ("every optional absent", every_optional_absent),
        ] {
            let json = serde_json::to_string(&manifest).unwrap();
            let parsed: SegmentManifest = serde_json::from_str(&json).unwrap();
            check!(parsed == manifest, "case {name}");
        }

        // An unknown field is a decode failure, not a silently dropped one.
        let with_extra = r#"{"body":{"format_version":1,"surprise":true},"signature":null}"#;
        check!(serde_json::from_str::<SegmentManifest>(with_extra).is_err());
    }

    #[test]
    fn manifest_signature_round_trips_and_rejects_tampering() {
        let (signer, public_key) = fresh_signer(KEY_ID);
        let base = sample_body();
        let authentic = sign_manifest(base.clone(), &signer);

        let json = serde_json::to_string(&authentic).unwrap();
        let reparsed: SegmentManifest = serde_json::from_str(&json).unwrap();
        check!(reparsed == authentic, "signature survives JSON");

        let mut tampered_body = authentic.clone();
        tampered_body.body.objects[0].sha256 = Sha256Digest::of(b"swapped body");

        let mut tampered_seq = authentic.clone();
        tampered_seq.body.chain.seq = ManifestSeq(base.chain.seq.0 + 1);

        // A signature made over a head that this body does not produce.
        let wrong_head_message = manifest_signing_bytes(
            KEY_ID,
            base.chain.epoch_id,
            base.chain.seq,
            ChainHead([0xab; 32]),
        );
        let tampered_head = SegmentManifest {
            body: base.clone(),
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(public_key.clone()),
                signature: HexBytes(signer.sign(&wrong_head_message)),
            }),
        };

        let mut tampered_key_id = authentic.clone();
        tampered_key_id.signature.as_mut().unwrap().key_id = "worm-key-2".to_string();

        let unsigned = SegmentManifest {
            body: base,
            signature: None,
        };

        for (name, manifest, expected) in [
            ("untampered", &reparsed, true),
            ("tampered body", &tampered_body, false),
            ("tampered seq", &tampered_seq, false),
            ("signature over another head", &tampered_head, false),
            ("tampered key id", &tampered_key_id, false),
            ("unsigned", &unsigned, false),
        ] {
            check!(
                verify_manifest_signature(manifest, &public_key) == expected,
                "case {name}"
            );
        }

        let (_other_signer, other_public_key) = fresh_signer("worm-key-2");
        check!(!verify_manifest_signature(&authentic, &other_public_key));
    }

    #[test]
    fn manifest_domain_differs_from_audit_checkpoint_domain() {
        check!(MANIFEST_DOMAIN != CHECKPOINT_DOMAIN);
        check!(MANIFEST_BODY_DOMAIN != CHECKPOINT_DOMAIN);
        check!(MANIFEST_DOMAIN != MANIFEST_BODY_DOMAIN);

        let body = sample_body();
        check!(canonical_manifest_bytes(&body).starts_with(MANIFEST_BODY_DOMAIN));
        let head = manifest_head(&body);
        let manifest_bytes =
            manifest_signing_bytes(KEY_ID, body.chain.epoch_id, body.chain.seq, head);
        check!(manifest_bytes.starts_with(MANIFEST_DOMAIN));
        check!(!manifest_bytes.starts_with(CHECKPOINT_DOMAIN));

        // Behaviourally: an audit checkpoint signature over this very head, by
        // this very key, is not a manifest signature.
        let (signer, public_key) = fresh_signer(KEY_ID);
        let checkpoint_bytes =
            checkpoint_signing_bytes(KEY_ID, Seq(body.chain.seq.0), &head.0, EpochMs(0));
        let cross_purpose = SegmentManifest {
            body,
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(public_key.clone()),
                signature: HexBytes(signer.sign(&checkpoint_bytes)),
            }),
        };
        check!(!verify_manifest_signature(&cross_purpose, &public_key));
    }

    #[test]
    fn hex_newtypes_reject_bad_input_without_panicking() {
        let head = ChainHead([0xa1; 32]);
        let encoded = serde_json::to_string(&head).unwrap();
        check!(encoded == format!("\"{}\"", "a1".repeat(32)));
        check!(serde_json::from_str::<ChainHead>(&encoded).unwrap() == head);
        check!(head.to_string() == "a1".repeat(32));
        check!(format!("{head:?}") == format!("ChainHead({})", "a1".repeat(32)));

        let digest = Sha256Digest::of(b"body");
        check!(
            serde_json::from_str::<Sha256Digest>(&serde_json::to_string(&digest).unwrap()).unwrap()
                == digest
        );
        check!(format!("{digest:?}") == format!("Sha256Digest({digest})"));

        for (name, json) in [
            ("empty string", "\"\"".to_string()),
            ("too short", format!("\"{}\"", "00".repeat(31))),
            ("too long", format!("\"{}\"", "00".repeat(33))),
            ("odd length", format!("\"{}0\"", "00".repeat(31))),
            ("non-hex characters", format!("\"{}\"", "zz".repeat(32))),
            ("not a string", "12345".to_string()),
            ("null", "null".to_string()),
            ("array", "[]".to_string()),
        ] {
            check!(
                serde_json::from_str::<ChainHead>(&json).is_err(),
                "ChainHead case {name}"
            );
            check!(
                serde_json::from_str::<Sha256Digest>(&json).is_err(),
                "Sha256Digest case {name}"
            );
        }

        for (name, json, expected) in [
            ("empty", "\"\"", Some(Vec::new())),
            (
                "even-length hex",
                "\"00ff10\"",
                Some(vec![0x00, 0xff, 0x10]),
            ),
            ("odd length", "\"abc\"", None),
            ("non-hex characters", "\"zz\"", None),
            ("not a string", "42", None),
            ("null", "null", None),
        ] {
            check!(
                serde_json::from_str::<HexBytes>(json).ok().map(|h| h.0) == expected,
                "HexBytes case {name}"
            );
        }

        check!(HexBytes(vec![0xde, 0xad]).to_string() == "dead");
        check!(format!("{:?}", HexBytes(vec![0xde, 0xad])) == "HexBytes(dead)");
    }

    #[test]
    fn genesis_head_is_the_audit_genesis() {
        check!(ChainHead::GENESIS.0 == crabka_audit::chain::GENESIS_HEAD);
    }

    #[test]
    fn segment_identity_flattens_metadata() {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(0x11), "orders", 3),
                Uuid::from_u128(0x22),
            ),
            100,
            199,
            1_713_000_000_000,
            7,
            1_713_000_001_000,
            RemoteLogSegmentDetails::new(
                4096,
                RemoteLogSegmentState::CopySegmentFinished,
                BTreeMap::from([(LeaderEpoch(0), 100), (LeaderEpoch(1), 150)]),
            ),
        )
        .unwrap()
        .with_txn_index_empty(true);

        check!(
            SegmentIdentity::from_metadata(&md)
                == SegmentIdentity {
                    topic: "orders".to_string(),
                    topic_id: Uuid::from_u128(0x11),
                    partition: 3,
                    segment_id: Uuid::from_u128(0x22),
                    start_offset: 100,
                    end_offset: 199,
                    max_timestamp_ms: 1_713_000_000_000,
                    broker_id: 7,
                    event_timestamp_ms: 1_713_000_001_000,
                    segment_size_bytes: 4096,
                    leader_epochs: BTreeMap::from([(0, 100), (1, 150)]),
                    txn_index_empty: true,
                }
        );
    }
}
