//! Shared diskless-WAL object, index, and disaster-recovery capture codecs.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::{BufMut, Bytes, BytesMut};
use krabka_audit::FileEd25519Signer;
use krabka_verified::{DisklessWalReplayAction, diskless_wal_replay_decision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::worm::{
    ChainHead, ChainStamp, EpochId, HexBytes, MANIFEST_FORMAT_VERSION, ManifestBody, ManifestSeq,
    ManifestSignature, ObjectEntry, SegmentIdentity, SegmentManifest, Sha256Digest,
    TrustedManifestKeys, manifest_head, manifest_signing_bytes, verify_manifest_signature,
};

const MAGIC: [u8; 4] = *b"CKWL";
const OBJECT_VERSION: u16 = 1;
const HEADER_LEN: usize = 6;
const TRAILER_LEN: usize = 8;
const OBJECT_ENTRY_LEN: usize = 48;

/// Stable keyed replay fence ignored by every diskless index projection.
pub const REPLAY_FENCE_KEY: &[u8] = b"__krabka_diskless_replay_fence";

/// Synthetic expected-head name used for a signed capture boundary.
pub const CAPTURE_HEAD_NAME: &str = "diskless-capture";

const CAPTURE_STATE_KEY: &str = "__krabka_diskless_capture_state";

/// One partition's byte range in a flushed object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
    pub max_timestamp_ms: i64,
}

/// Compaction key for one logical range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalIndexKey {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
}

impl WalIndexKey {
    const LEN: usize = 28;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        let mut out = Vec::with_capacity(Self::LEN);
        out.extend_from_slice(self.topic_id.as_bytes());
        out.extend_from_slice(&self.partition.to_be_bytes());
        out.extend_from_slice(&self.first_offset.to_be_bytes());
        out.into()
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::LEN] = bytes.try_into().ok()?;
        Some(Self {
            topic_id: Uuid::from_bytes(bytes[..16].try_into().ok()?),
            partition: i32::from_be_bytes(bytes[16..20].try_into().ok()?),
            first_offset: i64::from_be_bytes(bytes[20..].try_into().ok()?),
        })
    }
}

impl From<&WalIndexEntry> for WalIndexKey {
    fn from(value: &WalIndexEntry) -> Self {
        Self {
            topic_id: value.topic_id,
            partition: value.partition,
            first_offset: value.first_offset,
        }
    }
}

/// Compaction key for a partition delete floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalDeleteFloorKey {
    pub topic_id: Uuid,
    pub partition: i32,
}

impl WalDeleteFloorKey {
    const LEN: usize = 21;
    const TAG: u8 = 0xf0;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        let mut out = Vec::with_capacity(Self::LEN);
        out.push(Self::TAG);
        out.extend_from_slice(self.topic_id.as_bytes());
        out.extend_from_slice(&self.partition.to_be_bytes());
        out.into()
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::LEN] = bytes.try_into().ok()?;
        if bytes[0] != Self::TAG {
            return None;
        }
        Some(Self {
            topic_id: Uuid::from_bytes(bytes[1..17].try_into().ok()?),
            partition: i32::from_be_bytes(bytes[17..].try_into().ok()?),
        })
    }
}

/// Durable `DeleteRecords` floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalDeleteFloorRecord {
    pub topic_id: Uuid,
    pub partition: i32,
    pub floor: i64,
}

impl WalDeleteFloorRecord {
    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns the codec error when the record cannot be encoded.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map(Bytes::from)
            .map_err(|error| error.to_string())
    }

    /// Decode the index topic codec.
    ///
    /// # Errors
    /// Returns the codec error when `bytes` are malformed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|error| error.to_string())
    }
}

/// Durable index value for a flushed object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    pub const FORMAT_VERSION: u16 = 2;

    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns an error for an unsupported version or codec failure.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(format!(
                "unsupported diskless WAL index format version {}",
                self.format_version
            ));
        }
        <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map(Bytes::from)
            .map_err(|error| error.to_string())
    }

    /// Decode with strict format-version checking.
    ///
    /// # Errors
    /// Returns an error for malformed bytes or an unsupported version.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        match <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(bytes) {
            Ok(record) if record.format_version == Self::FORMAT_VERSION => Ok(record),
            Ok(record) => Err(format!(
                "unsupported diskless WAL index format version {}",
                record.format_version
            )),
            Err(error) => {
                if let Ok(previous) = <serde_wincode::SerdeCompat<PreviousWalFlushRecord> as wincode::Deserialize>::deserialize(bytes) {
                    return Err(format!(
                        "unsupported diskless WAL index format version {}",
                        previous.format_version
                    ));
                }
                Err(error.to_string())
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct PreviousWalFlushRecord {
    object_key: String,
    format_version: u16,
    entries: Vec<PreviousWalIndexEntry>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct PreviousWalIndexEntry {
    topic_id: Uuid,
    partition: i32,
    first_offset: i64,
    last_offset: i64,
    byte_start: u64,
    byte_len: u32,
}

/// One live capture range and its authoritative object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedWalRange {
    pub object_key: String,
    pub entry: WalIndexEntry,
}

/// Captured committed state for one data partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisklessPartitionCapture {
    pub topic: String,
    pub topic_id: Uuid,
    pub partition: i32,
    pub delete_floor: i64,
    pub recovery_cutoff: i64,
    pub ranges: Vec<CapturedWalRange>,
}

/// Portable committed diskless-WAL capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisklessWalCapture {
    pub format_version: u16,
    pub captured_at_ms: u64,
    pub source_cutoffs: Vec<i64>,
    pub partitions: Vec<DisklessPartitionCapture>,
    /// Optional signed WORM boundary covering this capture and its WAL objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SegmentManifest>,
}

impl DisklessWalCapture {
    pub const FORMAT_VERSION: u16 = 1;

    /// Decode JSON and reject unknown capture versions.
    ///
    /// # Errors
    /// Returns an error for malformed JSON, an unsupported version, or invalid state.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, String> {
        let capture: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if capture.format_version != Self::FORMAT_VERSION {
            return Err(format!(
                "unsupported diskless WAL capture format version {}",
                capture.format_version
            ));
        }
        capture.validate()?;
        Ok(capture)
    }

    /// Validate all state that controls restore selection and boundaries.
    ///
    /// # Errors
    /// Returns an error for invalid partitions, ranges, floors, or cutoffs.
    pub fn validate(&self) -> Result<(), String> {
        if self.source_cutoffs.iter().any(|cutoff| *cutoff < 0) {
            return Err("negative diskless WAL source cutoff".to_owned());
        }
        let mut partitions = std::collections::HashSet::new();
        for partition in &self.partitions {
            if partition.partition < 0
                || partition.delete_floor < 0
                || partition.recovery_cutoff < partition.delete_floor
                || !partitions.insert((partition.topic_id, partition.partition))
            {
                return Err(format!(
                    "invalid or duplicate diskless capture partition {}-{}",
                    partition.topic, partition.partition
                ));
            }
            let mut previous_last = None;
            for range in &partition.ranges {
                if range.object_key.is_empty()
                    || range.object_key == CAPTURE_STATE_KEY
                    || range.entry.topic_id != partition.topic_id
                    || range.entry.partition != partition.partition
                    || range.entry.first_offset < 0
                    || range.entry.last_offset < range.entry.first_offset
                    || range.entry.byte_len == 0
                    || previous_last.is_some_and(|last| last >= range.entry.first_offset)
                {
                    return Err(format!(
                        "invalid or unordered diskless WAL range for {}-{}",
                        partition.topic, partition.partition
                    ));
                }
                previous_last = Some(range.entry.last_offset);
            }
            let observed_cutoff = previous_last
                .map(|last| {
                    last.checked_add(1)
                        .ok_or_else(|| "diskless WAL recovery cutoff overflow".to_owned())
                })
                .transpose()?
                .unwrap_or(partition.delete_floor)
                .max(partition.delete_floor);
            if observed_cutoff != partition.recovery_cutoff {
                return Err(format!(
                    "diskless WAL ranges for {}-{} end at {observed_cutoff}, capture declares {}",
                    partition.topic, partition.partition, partition.recovery_cutoff
                ));
            }
        }
        Ok(())
    }

    fn unsigned_bytes(&self) -> Result<Vec<u8>, String> {
        let mut unsigned = self.clone();
        unsigned.authentication = None;
        serde_json::to_vec(&unsigned).map_err(|error| error.to_string())
    }

    /// Sign the exact capture state and referenced WAL object claims.
    ///
    /// # Errors
    /// Returns an error when capture state is invalid, object claims do not exactly cover it,
    /// or its deterministic encoding fails.
    pub fn seal(
        &mut self,
        mut objects: Vec<ObjectEntry>,
        signer: &FileEd25519Signer,
    ) -> Result<ChainHead, String> {
        self.validate()?;
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        let actual = objects
            .iter()
            .map(|object| object.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != actual || objects.len() != actual.len() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        let state = self.unsigned_bytes()?;
        let state_digest = Sha256Digest::of(&state);
        objects.push(ObjectEntry {
            suffix: ".capture-state".to_owned(),
            key: CAPTURE_STATE_KEY.to_owned(),
            size_bytes: state.len() as u64,
            sha256: state_digest,
            e_tag: None,
            version_id: None,
            create_precondition: false,
        });
        let mut segment_id = [0; 16];
        segment_id.copy_from_slice(&state_digest.0[..16]);
        let identity = SegmentIdentity {
            topic: CAPTURE_HEAD_NAME.to_owned(),
            topic_id: Uuid::nil(),
            partition: 0,
            segment_id: Uuid::from_bytes(segment_id),
            start_offset: 0,
            end_offset: 0,
            max_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            broker_id: -1,
            event_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            segment_size_bytes: objects
                .iter()
                .map(|object| object.size_bytes)
                .sum::<u64>()
                .try_into()
                .unwrap_or(i64::MAX),
            leader_epochs: BTreeMap::new(),
            txn_index_empty: true,
        };
        let chain = ChainStamp {
            epoch_id: EpochId(Uuid::nil()),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let body = ManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            segment: identity,
            objects,
            chain,
        };
        let head = manifest_head(&body);
        let message = manifest_signing_bytes(signer.key_id(), chain.epoch_id, chain.seq, head);
        self.authentication = Some(SegmentManifest {
            body,
            signature: Some(ManifestSignature {
                key_id: signer.key_id().to_owned(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }),
        });
        Ok(head)
    }

    /// Verify the embedded signed boundary and return trusted WAL claims.
    ///
    /// # Errors
    /// Returns an error for invalid capture state, missing or untrusted evidence, a bad
    /// signature, a head mismatch, or incomplete object claims.
    pub fn authenticate(
        &self,
        trusted: &TrustedManifestKeys,
        expected_head: Option<&str>,
    ) -> Result<BTreeMap<String, ObjectEntry>, String> {
        self.validate()?;
        let manifest = self
            .authentication
            .as_ref()
            .ok_or_else(|| "diskless capture has no signed WORM boundary".to_owned())?;
        if manifest.body.format_version != MANIFEST_FORMAT_VERSION {
            return Err("unsupported diskless capture manifest version".to_owned());
        }
        let signature = manifest
            .signature
            .as_ref()
            .ok_or_else(|| "diskless capture WORM boundary is unsigned".to_owned())?;
        let key = trusted
            .get(&signature.key_id)
            .ok_or_else(|| format!("diskless capture uses untrusted key `{}`", signature.key_id))?;
        if !verify_manifest_signature(manifest, key) {
            return Err("diskless capture WORM signature is invalid".to_owned());
        }
        let head = manifest_head(&manifest.body).to_string();
        let expected_head = expected_head.ok_or_else(|| {
            format!("trusted diskless restore requires --worm-expect-head {CAPTURE_HEAD_NAME}=HEX")
        })?;
        if !head.eq_ignore_ascii_case(expected_head) {
            return Err(format!(
                "pinned diskless capture head is {expected_head}, authenticated head is {head}"
            ));
        }
        let state = self.unsigned_bytes()?;
        let mut claims = BTreeMap::new();
        let mut state_claim = None;
        for object in &manifest.body.objects {
            if object.key == CAPTURE_STATE_KEY {
                if state_claim.replace(object).is_some() {
                    return Err("duplicate signed diskless capture-state claim".to_owned());
                }
            } else if claims.insert(object.key.clone(), object.clone()).is_some() {
                return Err(format!("duplicate signed WAL claim `{}`", object.key));
            }
        }
        let state_claim = state_claim
            .ok_or_else(|| "signed diskless capture-state claim is missing".to_owned())?;
        if state_claim.size_bytes != state.len() as u64
            || state_claim.sha256 != Sha256Digest::of(&state)
        {
            return Err("diskless capture state changed after signing".to_owned());
        }
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != claims.keys().map(String::as_str).collect() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        Ok(claims)
    }
}

/// Reducer for committed keyed index events.
#[derive(Default)]
pub struct WalCaptureProjection {
    ranges: HashMap<WalIndexKey, CapturedWalRange>,
    floors: HashMap<(Uuid, i32), i64>,
    keyed_ranges: HashSet<WalIndexKey>,
    replay_tombstones: HashSet<WalIndexKey>,
    legacy_replay_finished: bool,
}

impl WalCaptureProjection {
    /// Apply one committed keyed value or tombstone.
    ///
    /// # Errors
    /// Returns an error for malformed keys, values, or inconsistent records.
    pub fn apply(&mut self, key: Option<&[u8]>, value: Option<&[u8]>) -> Result<(), String> {
        let Some(key) = key else {
            let value =
                value.ok_or_else(|| "legacy diskless WAL tombstone has no key".to_owned())?;
            let record = WalFlushRecord::from_bytes(value)?;
            for entry in record.entries {
                Self::validate_range(&entry)?;
                let range_key = WalIndexKey::from(&entry);
                let decision = diskless_wal_replay_decision(
                    0,
                    self.keyed_ranges.contains(&range_key),
                    self.replay_tombstones.contains(&range_key),
                    self.legacy_replay_finished,
                );
                if decision.action == DisklessWalReplayAction::Store {
                    self.store_range(range_key, record.object_key.clone(), entry);
                }
            }
            return Ok(());
        };
        if let Some(floor_key) = WalDeleteFloorKey::from_bytes(key) {
            if let Some(value) = value {
                let record = WalDeleteFloorRecord::from_bytes(value)?;
                if record.topic_id != floor_key.topic_id
                    || record.partition != floor_key.partition
                    || record.floor < 0
                {
                    return Err("diskless WAL delete-floor key/value mismatch".to_owned());
                }
                let floor = self
                    .floors
                    .entry((record.topic_id, record.partition))
                    .or_default();
                *floor = (*floor).max(record.floor);
            } else {
                self.floors
                    .remove(&(floor_key.topic_id, floor_key.partition));
            }
            return Ok(());
        }
        let range_key = WalIndexKey::from_bytes(key)
            .ok_or_else(|| "invalid diskless WAL index key".to_owned())?;
        let Some(value) = value else {
            let decision = diskless_wal_replay_decision(
                2,
                self.keyed_ranges.contains(&range_key),
                self.replay_tombstones.contains(&range_key),
                self.legacy_replay_finished,
            );
            self.set_replay_markers(range_key, decision.keyed_range, decision.replay_tombstone);
            if decision.action == DisklessWalReplayAction::Remove {
                self.ranges.remove(&range_key);
            }
            return Ok(());
        };
        let record = WalFlushRecord::from_bytes(value)?;
        let entry = record
            .entries
            .into_iter()
            .find(|entry| WalIndexKey::from(entry) == range_key)
            .ok_or_else(|| "diskless WAL index key/value mismatch".to_owned())?;
        Self::validate_range(&entry)?;
        let decision = diskless_wal_replay_decision(
            1,
            self.keyed_ranges.contains(&range_key),
            self.replay_tombstones.contains(&range_key),
            self.legacy_replay_finished,
        );
        self.set_replay_markers(range_key, decision.keyed_range, decision.replay_tombstone);
        self.store_range(range_key, record.object_key, entry);
        Ok(())
    }

    fn validate_range(entry: &WalIndexEntry) -> Result<(), String> {
        if entry.first_offset < 0 || entry.last_offset < entry.first_offset || entry.byte_len == 0 {
            return Err("invalid diskless WAL index range".to_owned());
        }
        Ok(())
    }

    fn set_replay_markers(&mut self, key: WalIndexKey, keyed: bool, tombstone: bool) {
        if keyed {
            self.keyed_ranges.insert(key);
        } else {
            self.keyed_ranges.remove(&key);
        }
        if tombstone {
            self.replay_tombstones.insert(key);
        } else {
            self.replay_tombstones.remove(&key);
        }
    }

    fn store_range(&mut self, key: WalIndexKey, object_key: String, entry: WalIndexEntry) {
        self.ranges
            .insert(key, CapturedWalRange { object_key, entry });
    }

    /// Stop accepting legacy records after every replay partition reaches its fence.
    pub fn finish_legacy_replay(&mut self) {
        self.replay_tombstones.clear();
        self.legacy_replay_finished = true;
    }

    /// Freeze the projection into a deterministic portable capture.
    ///
    /// # Errors
    /// Returns an error for unknown topic ids or inconsistent live ranges.
    pub fn capture<S: std::hash::BuildHasher>(
        &self,
        topic_names: &HashMap<Uuid, String, S>,
        source_cutoffs: Vec<i64>,
        captured_at_ms: u64,
    ) -> Result<DisklessWalCapture, String> {
        let mut grouped: BTreeMap<(String, Uuid, i32), Vec<CapturedWalRange>> = BTreeMap::new();
        for range in self.ranges.values() {
            let topic = topic_names.get(&range.entry.topic_id).ok_or_else(|| {
                format!(
                    "no live topic names diskless WAL topic id {}",
                    range.entry.topic_id
                )
            })?;
            grouped
                .entry((topic.clone(), range.entry.topic_id, range.entry.partition))
                .or_default()
                .push(range.clone());
        }
        for (topic_id, partition) in self.floors.keys() {
            let topic = topic_names
                .get(topic_id)
                .ok_or_else(|| format!("no live topic names diskless WAL topic id {topic_id}"))?;
            grouped
                .entry((topic.clone(), *topic_id, *partition))
                .or_default();
        }
        let mut partitions = Vec::with_capacity(grouped.len());
        for ((topic, topic_id, partition), mut ranges) in grouped {
            ranges.sort_by_key(|range| range.entry.first_offset);
            if ranges
                .windows(2)
                .any(|pair| pair[0].entry.last_offset >= pair[1].entry.first_offset)
            {
                return Err(format!(
                    "overlapping diskless WAL ranges for {topic}-{partition}"
                ));
            }
            let mut runs: Vec<CapturedWalRange> = Vec::with_capacity(ranges.len());
            for range in ranges {
                let joins_previous = runs.last().is_some_and(|previous| {
                    previous.object_key == range.object_key
                        && previous.entry.last_offset.checked_add(1)
                            == Some(range.entry.first_offset)
                        && previous
                            .entry
                            .byte_start
                            .checked_add(u64::from(previous.entry.byte_len))
                            == Some(range.entry.byte_start)
                });
                if joins_previous {
                    if let Some(previous) = runs.last_mut() {
                        previous.entry.last_offset = range.entry.last_offset;
                        previous.entry.byte_len = previous
                            .entry
                            .byte_len
                            .checked_add(range.entry.byte_len)
                            .ok_or_else(|| {
                                format!("diskless WAL run exceeds 4 GiB for {topic}-{partition}")
                            })?;
                        previous.entry.max_timestamp_ms = previous
                            .entry
                            .max_timestamp_ms
                            .max(range.entry.max_timestamp_ms);
                    }
                } else {
                    runs.push(range);
                }
            }
            let ranges = runs;
            let delete_floor = self
                .floors
                .get(&(topic_id, partition))
                .copied()
                .unwrap_or(0);
            let recovery_cutoff = ranges
                .last()
                .map_or(delete_floor, |range| {
                    range.entry.last_offset.saturating_add(1)
                })
                .max(delete_floor);
            partitions.push(DisklessPartitionCapture {
                topic,
                topic_id,
                partition,
                delete_floor,
                recovery_cutoff,
                ranges,
            });
        }
        Ok(DisklessWalCapture {
            format_version: DisklessWalCapture::FORMAT_VERSION,
            captured_at_ms,
            source_cutoffs,
            partitions,
            authentication: None,
        })
    }
}

/// One run recorded by a CKWL footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
}

/// Invalid combined diskless-WAL object framing.
#[derive(Debug, thiserror::Error)]
pub enum WalObjectError {
    /// Object does not contain the minimum header and trailer.
    #[error("wal object too short")]
    TooShort,
    /// Header or trailer magic is invalid.
    #[error("bad wal object magic")]
    BadMagic,
    /// Object uses an unsupported framing version.
    #[error("unsupported wal object version {0}")]
    BadVersion(u16),
    /// Footer length, entry encoding, or byte ranges are invalid.
    #[error("corrupt wal object manifest")]
    BadManifest,
}

/// Builder for combined CKWL objects.
#[derive(Default)]
pub struct WalObjectBuilder {
    body: BytesMut,
    entries: Vec<WalObjectEntry>,
}

impl WalObjectBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn body_len(&self) -> usize {
        self.body.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Append one contiguous partition run.
    ///
    /// # Panics
    /// Panics only if an in-memory object exceeds platform or CKWL length limits.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) {
        let byte_start = u64::try_from(HEADER_LEN + self.body.len()).expect("object size fits u64");
        let byte_len = u32::try_from(run.len()).expect("run size fits u32");
        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
    }

    #[must_use]
    /// # Panics
    /// Panics only if the in-memory footer exceeds 4 GiB.
    pub fn finish(self) -> Bytes {
        let mut out = BytesMut::with_capacity(
            HEADER_LEN + self.body.len() + self.entries.len() * OBJECT_ENTRY_LEN + TRAILER_LEN,
        );
        out.extend_from_slice(&MAGIC);
        out.put_u16_le(OBJECT_VERSION);
        out.extend_from_slice(&self.body);
        let footer_start = out.len();
        for entry in self.entries {
            out.extend_from_slice(entry.topic_id.as_bytes());
            out.put_i32_le(entry.partition);
            out.put_i64_le(entry.first_offset);
            out.put_i64_le(entry.last_offset);
            out.put_u64_le(entry.byte_start);
            out.put_u32_le(entry.byte_len);
        }
        out.put_u32_le(u32::try_from(out.len() - footer_start).expect("footer fits u32"));
        out.extend_from_slice(&MAGIC);
        out.freeze()
    }

    /// Append one run and finish the object.
    #[must_use]
    pub fn finish_with_run(
        mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Bytes {
        self.append_run(topic_id, partition, first_offset, last_offset, run);
        self.finish()
    }
}

/// Parse and validate a CKWL footer.
///
/// # Errors
/// Returns an error for malformed framing, entries, versions, or byte ranges.
pub fn parse_wal_object(object: &Bytes) -> Result<Vec<WalObjectEntry>, WalObjectError> {
    if object.len() < HEADER_LEN + TRAILER_LEN {
        return Err(WalObjectError::TooShort);
    }
    if object[..4] != MAGIC || object[object.len() - 4..] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let version = u16::from_le_bytes([object[4], object[5]]);
    if version != OBJECT_VERSION {
        return Err(WalObjectError::BadVersion(version));
    }
    let trailer = object.len() - TRAILER_LEN;
    let footer_len = usize::try_from(u32::from_le_bytes(
        object[trailer..trailer + 4]
            .try_into()
            .map_err(|_| WalObjectError::BadManifest)?,
    ))
    .map_err(|_| WalObjectError::BadManifest)?;
    if footer_len % OBJECT_ENTRY_LEN != 0 {
        return Err(WalObjectError::BadManifest);
    }
    let footer = trailer
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .ok_or(WalObjectError::BadManifest)?;
    let mut entries = Vec::with_capacity(footer_len / OBJECT_ENTRY_LEN);
    for raw in object[footer..trailer].as_chunks::<OBJECT_ENTRY_LEN>().0 {
        let entry = WalObjectEntry {
            topic_id: Uuid::from_slice(&raw[..16]).map_err(|_| WalObjectError::BadManifest)?,
            partition: i32::from_le_bytes(
                raw[16..20]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            first_offset: i64::from_le_bytes(
                raw[20..28]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            last_offset: i64::from_le_bytes(
                raw[28..36]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_start: u64::from_le_bytes(
                raw[36..44]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_len: u32::from_le_bytes(
                raw[44..48]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
        };
        let start = usize::try_from(entry.byte_start).map_err(|_| WalObjectError::BadManifest)?;
        let end = start
            .checked_add(usize::try_from(entry.byte_len).map_err(|_| WalObjectError::BadManifest)?)
            .filter(|end| *end <= footer)
            .ok_or(WalObjectError::BadManifest)?;
        let _ = end;
        if start < HEADER_LEN {
            return Err(WalObjectError::BadManifest);
        }
        entries.push(entry);
    }
    Ok(entries)
}

/// Slice one parsed run without copying.
///
/// # Panics
/// Panics if `entry` did not come from successfully parsing `object`.
#[must_use]
pub fn run_bytes(object: &Bytes, entry: &WalObjectEntry) -> Bytes {
    let start = usize::try_from(entry.byte_start).expect("parsed byte start fits usize");
    let len = usize::try_from(entry.byte_len).expect("parsed byte length fits usize");
    object.slice(start..start + len)
}
