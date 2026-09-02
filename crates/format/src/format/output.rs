//! The files a format leaves in the log directory.
//!
//! A formatted directory holds `meta.properties.json`, the bootstrap manifest
//! and its binary record stream, and — for a dynamic KIP-853 format — the
//! offset-zero metadata checkpoint. Each writer serializes records the run has
//! already resolved, so the encoding and the I/O sit together here, apart from
//! the flag handling that decides what goes in them.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use krabka_metadata::MetadataRecord;
use serde::Serialize;
use serde_wincode::SerdeCompat;
use wincode::Serialize as _;

use crate::ids::{ClusterId, DirectoryId};

pub(super) const ZERO_CHECKPOINT_NAME: &str = "00000000000000000000-0000000000.checkpoint";

/// Persist `meta.properties.json` — the broker recovers `directory_id`
/// from it on every boot (KIP-853 voter identity).
#[tracing::instrument(
    level = "debug",
    name = "cli.write_meta_properties",
    skip_all,
    fields(log_dir = %log_dir.display(), %cluster_id, %directory_id),
    err
)]
pub(super) fn write_meta_properties(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    directory_id: DirectoryId,
) -> Result<(), String> {
    let meta = serde_json::json!({
        "cluster_id": cluster_id.to_string(),
        "directory_id": directory_id.to_string(),
        "version": 1,
    });
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| format!("serialize meta.properties.json: {e}"))?;
    std::fs::write(log_dir.join(super::META_PROPERTIES), bytes)
        .map_err(|e| format!("write meta.properties.json: {e}"))
}

/// Human-readable manifest written to `<log_dir>/bootstrap.json`.
#[derive(Debug, Serialize)]
struct BootstrapManifest {
    /// Schema version of this bootstrap manifest. Bumped if the layout
    /// changes; the broker's future consumer will reject unknown values.
    schema: u32,
    // `ClusterId` is `#[serde(transparent)]`, so this serializes as the bare
    // UUID string exactly as the previous `Uuid` field did.
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
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    control_records: &[MetadataRecord],
    metadata_records: &[MetadataRecord],
) -> Result<(), String> {
    let mut image = krabka_metadata::MetadataImage::new(cluster_id.into());
    for record in control_records.iter().chain(metadata_records) {
        image.apply(record);
    }
    let bytes = krabka_raft::serialize_metadata_snapshot(&image, 0)
        .map_err(|e| format!("serialize offset-zero checkpoint: {e}"))?;
    let checkpoint_dir = krabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"));
    std::fs::create_dir_all(&checkpoint_dir)
        .map_err(|e| format!("create checkpoint directory: {e}"))?;
    std::fs::write(checkpoint_dir.join(ZERO_CHECKPOINT_NAME), bytes)
        .map_err(|e| format!("write offset-zero checkpoint: {e}"))
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
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    records: &[MetadataRecord],
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
    std::fs::write(log_dir.join("bootstrap.records.bin"), &bin)
        .map_err(|e| format!("write bootstrap.records.bin: {e}"))?;

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
    std::fs::write(log_dir.join("bootstrap.json"), json)
        .map_err(|e| format!("write bootstrap.json: {e}"))?;

    Ok(())
}
