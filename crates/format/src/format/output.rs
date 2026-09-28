//! The files a format leaves in a log directory.
//!
//! Every formatted directory holds `meta.properties.json` and the bootstrap
//! manifest with its binary record stream. The metadata log directory of a
//! dynamic KIP-853 format also holds the offset-zero metadata checkpoint. Each
//! writer serializes records the run has already resolved, so the encoding and
//! the I/O sit together here, apart from the flag handling that decides what
//! goes in them.

use std::{ffi::OsString, io::Write as _, path::Path};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use krabka_metadata::MetadataRecord;
use serde::Serialize;
use serde_wincode::SerdeCompat;
use wincode::Serialize as _;

use crate::ids::{ClusterId, DirectoryId};

pub(super) const ZERO_CHECKPOINT_NAME: &str = "00000000000000000000-0000000000.checkpoint";

/// The format stamp of `meta.properties.json`. Version 3 stores both ids in
/// Kafka's 22-character base64 form. The broker refuses any other stamp.
pub const META_PROPERTIES_VERSION: u64 = 3;

/// The name `meta.properties.json` has while it is written, before the rename
/// that publishes it.
pub(super) const META_PROPERTIES_TMP: &str = "meta.properties.json.tmp";

/// The environment variable that makes a run fail after it writes the file of
/// the given name. It is a test seam: it is how the tests interrupt a run
/// partway through, to show that the next run recovers.
pub const FAIL_AFTER_ENV: &str = "KRABKA_FORMAT_FAIL_AFTER";

/// A failure injected through [`FAIL_AFTER_ENV`], or `None` in normal use.
#[derive(Debug, Clone, Default)]
pub(super) struct Fault(Option<OsString>);

impl Fault {
    /// Reads [`FAIL_AFTER_ENV`].
    pub(super) fn from_env() -> Self {
        Self(std::env::var_os(FAIL_AFTER_ENV))
    }

    /// Fails when `path` is the file the fault names.
    fn after(&self, path: &Path) -> Result<(), String> {
        match (&self.0, path.file_name()) {
            (Some(name), Some(written)) if name == written => Err(format!(
                "injected failure after {} ({FAIL_AFTER_ENV})",
                path.display()
            )),
            _ => Ok(()),
        }
    }
}

/// The content of `meta.properties.json`.
#[derive(Serialize)]
struct MetaPropertiesFile {
    cluster_id: ClusterId,
    directory_id: DirectoryId,
    version: u64,
}

/// Writes `meta.properties.json`: the marker of a formatted directory, and the
/// file the broker reads its cluster and directory ids from on every boot.
///
/// The file is written under a temporary name, synced, and renamed into
/// place, as Kafka's `PropertiesUtils.writePropertiesFile` does. A run that
/// stops partway therefore never leaves a truncated marker.
#[tracing::instrument(
    level = "debug",
    name = "cli.write_meta_properties",
    skip_all,
    fields(log_dir = %log_dir.display(), %cluster_id, %directory_id),
    err
)]
pub(super) fn write_meta_properties(
    log_dir: &Path,
    cluster_id: ClusterId,
    directory_id: DirectoryId,
    fault: &Fault,
) -> Result<(), String> {
    let meta = MetaPropertiesFile {
        cluster_id,
        directory_id,
        version: META_PROPERTIES_VERSION,
    };
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| format!("serialize meta.properties.json: {e}"))?;
    let tmp = log_dir.join(META_PROPERTIES_TMP);
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()
    };
    write().map_err(|e| format!("write {}: {e}", tmp.display()))?;
    fault.after(&tmp)?;
    let path = log_dir.join(super::META_PROPERTIES);
    std::fs::rename(&tmp, &path).map_err(|e| format!("write {}: {e}", path.display()))?;
    fault.after(&path)
}

/// Human-readable manifest written to `<log_dir>/bootstrap.json`.
#[derive(Debug, Serialize)]
struct BootstrapManifest {
    /// Schema version of this bootstrap manifest. Bumped if the layout
    /// changes; the broker's future consumer will reject unknown values.
    schema: u32,
    /// Kafka's 22-character base64 form.
    cluster_id: ClusterId,
    record_count: usize,
    /// Base64-encoded `SerdeCompat<MetadataRecord>` payloads, one per
    /// seed record. Mirrors the contents of `bootstrap.records.bin` so
    /// operators can inspect the file without a hex editor.
    records_b64: Vec<String>,
}

/// Write the authoritative KIP-630/KIP-853 offset-zero checkpoint for a
/// dynamically formatted controller.
pub(super) fn write_dynamic_checkpoint(
    log_dir: &Path,
    cluster_id: ClusterId,
    control_records: &[MetadataRecord],
    metadata_records: &[MetadataRecord],
    fault: &Fault,
) -> Result<(), String> {
    let mut image = krabka_metadata::MetadataImage::new(cluster_id.into());
    for record in control_records.iter().chain(metadata_records) {
        image.apply(record);
    }
    let bytes = krabka_raft::serialize_metadata_snapshot(&image, 0)
        .map_err(|e| format!("serialize offset-zero checkpoint: {e}"))?;
    let checkpoint_dir =
        krabka_raft::kraft::checkpoint_dir(&log_dir.join(super::ensemble::CLUSTER_METADATA));
    std::fs::create_dir_all(&checkpoint_dir)
        .map_err(|e| format!("create checkpoint directory: {e}"))?;
    let path = checkpoint_dir.join(ZERO_CHECKPOINT_NAME);
    std::fs::write(&path, bytes).map_err(|e| format!("write offset-zero checkpoint: {e}"))?;
    fault.after(&path)
}

/// Serialize the manifest + records to disk under `log_dir`. Returns the
/// first I/O or encoding error encountered.
#[tracing::instrument(
    level = "debug",
    name = "cli.write_bootstrap_files",
    skip_all,
    fields(log_dir = %log_dir.display(), record_count = records.len()),
    err
)]
pub(super) fn write_bootstrap_files(
    log_dir: &Path,
    cluster_id: ClusterId,
    records: &[MetadataRecord],
    fault: &Fault,
) -> Result<(), String> {
    // 1. Per-record `SerdeCompat<MetadataRecord>` payloads.
    let mut record_blobs: Vec<Vec<u8>> = Vec::with_capacity(records.len());
    for rec in records {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec)
            .map_err(|e| format!("serialize record: {e}"))?;
        record_blobs.push(bytes);
    }

    // 2. Binary stream: length-prefixed (u32 LE) blobs, concatenated.
    let mut bin = Vec::new();
    for blob in &record_blobs {
        let len: u32 = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bin.extend_from_slice(&len.to_le_bytes());
        bin.extend_from_slice(blob);
    }
    let bin_path = log_dir.join("bootstrap.records.bin");
    std::fs::write(&bin_path, &bin).map_err(|e| format!("write bootstrap.records.bin: {e}"))?;
    fault.after(&bin_path)?;

    // 3. Manifest JSON (cluster id + base64 mirrors of each blob).
    let records_b64: Vec<String> = record_blobs.iter().map(|b| STANDARD.encode(b)).collect();
    let manifest = BootstrapManifest {
        schema: 1,
        cluster_id,
        record_count: records.len(),
        records_b64,
    };
    let json =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    let json_path = log_dir.join("bootstrap.json");
    std::fs::write(&json_path, json).map_err(|e| format!("write bootstrap.json: {e}"))?;
    fault.after(&json_path)
}
