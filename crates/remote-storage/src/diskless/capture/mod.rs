//! Portable disaster-recovery capture state and validation.

mod authentication;
mod projection;

pub use projection::WalCaptureProjection;
use {
    super::WalIndexEntry,
    crate::worm::{SegmentManifest, Sha256Digest},
    serde::{Deserialize, Serialize},
    uuid::Uuid,
};

/// Synthetic expected-head name used for a signed capture boundary.
pub const CAPTURE_HEAD_NAME: &str = "diskless-capture";

const CAPTURE_STATE_KEY: &str = "__krabka_diskless_capture_state";

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
    /// Digest of the controller checkpoint captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_snapshot_sha256: Option<Sha256Digest>,
    /// Digest of the RLMM cache snapshot captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rlmm_snapshot_sha256: Option<Sha256Digest>,
    /// Digest of the consumer-group offsets captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_offsets_sha256: Option<Sha256Digest>,
    /// Optional signed WORM boundary covering this capture and its WAL objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SegmentManifest>,
}

/// Just the version of a capture, read before the rest of it.
#[derive(Deserialize)]
struct DisklessWalCaptureVersion {
    format_version: Option<serde_json::Value>,
}

impl DisklessWalCapture {
    /// Version of `diskless-wal-index.json`, its required top-level
    /// `"format_version"` field.
    ///
    /// Part of the 1.x on-disk contract: a 1.x restore reads every capture
    /// that any earlier 1.x build wrote.
    pub const FORMAT_VERSION: u16 = 1;

    /// Decode JSON and reject a missing or unknown capture version.
    ///
    /// The version is read on its own first, so a capture of another version
    /// is reported as that, and not as whatever field its shape lacks.
    ///
    /// # Errors
    /// Returns an error for malformed JSON, a missing or unsupported version,
    /// or invalid state.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, String> {
        let probe: DisklessWalCaptureVersion =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        match probe.format_version {
            None => {
                return Err(
                    "diskless WAL capture has no format_version: it predates krabka \
                     1.0, which this build does not read"
                        .to_owned(),
                );
            }
            Some(version) if version != Self::FORMAT_VERSION => {
                return Err(format!(
                    "unsupported diskless WAL capture format version {version}: this build \
                     reads version {}",
                    Self::FORMAT_VERSION
                ));
            }
            Some(_) => {}
        }
        let capture: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
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
            if !valid_topic_name(&partition.topic)
                || partition.partition < 0
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
            let mut covered_through = partition.delete_floor;
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
                if range.entry.last_offset >= partition.delete_floor {
                    if range.entry.first_offset > covered_through {
                        return Err(format!(
                            "diskless WAL ranges for {}-{} have a gap at {covered_through}",
                            partition.topic, partition.partition
                        ));
                    }
                    covered_through = range
                        .entry
                        .last_offset
                        .checked_add(1)
                        .ok_or_else(|| "diskless WAL recovery cutoff overflow".to_owned())?;
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
}

fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
