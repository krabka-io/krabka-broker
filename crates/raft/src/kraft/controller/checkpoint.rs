//! The KIP-630 `.checkpoint` artifacts under the controller's metadata
//! directory: their filename encoding, atomic write, id ordering, and the
//! single-snapshot retention the engine keeps the directory at.

use crate::error::RaftError;

/// Write a KIP-630 `.checkpoint` artifact (bytes only) directly with
/// temp+rename atomicity.
pub fn write_checkpoint(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
    bytes: &[u8],
) -> Result<(), RaftError> {
    std::fs::create_dir_all(dir).map_err(krabka_log::LogError::Io)?;
    let name = checkpoint_name(end_offset, epoch);
    let path = dir.join(name);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(krabka_log::LogError::Io)?;
    std::fs::rename(&tmp, &path).map_err(krabka_log::LogError::Io)?;
    Ok(())
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts, pick the highest
/// `(end_offset, epoch)`, and return its raw bytes. Returns `None` when the
/// directory is absent or holds no checkpoint. Unlike `snapshot::load_latest`,
/// this reads only the `.checkpoint` (the `.meta` sidecar is gone in
/// this engine — the durable epoch lives in the quorum-state file).
pub fn load_latest_checkpoint(dir: &std::path::Path) -> Result<Option<Vec<u8>>, RaftError> {
    let Some((end_offset, epoch)) = latest_checkpoint_id(dir) else {
        return Ok(None);
    };
    let bytes = std::fs::read(dir.join(checkpoint_name(end_offset, epoch)))
        .map_err(krabka_log::LogError::Io)?;
    Ok(Some(bytes))
}

fn checkpoint_name(end_offset: i64, epoch: i32) -> String {
    format!("{end_offset:020}-{epoch:010}.checkpoint")
}

pub(crate) fn parse_checkpoint_name(name: &str) -> Option<(i64, i32)> {
    let stem = name.strip_suffix(".checkpoint")?;
    let (off, ep) = stem.split_once('-')?;
    Some((off.parse().ok()?, ep.parse().ok()?))
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` id, or `None` when the directory is absent or
/// holds no checkpoint.
pub fn latest_checkpoint_id(dir: &std::path::Path) -> Option<(i64, i32)> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut best: Option<(i64, i32)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((off, ep)) = parse_checkpoint_name(name) else {
            continue;
        };
        if best.is_none_or(|cur| checkpoint_id_is_newer((off, ep), cur)) {
            best = Some((off, ep));
        }
    }
    best
}

pub fn checkpoint_id_is_newer(candidate: (i64, i32), current: (i64, i32)) -> bool {
    matches!(candidate.cmp(&current), std::cmp::Ordering::Greater)
}

/// Delete every `.checkpoint` in `dir` except the latest `(end_offset, epoch)`,
/// keeping the checkpoint directory single-snapshot after a snapshot+prune or
/// install. Best-effort: read/remove errors are ignored.
pub fn retain_latest_checkpoint(dir: &std::path::Path) {
    let Some(latest) = latest_checkpoint_id(dir) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((off, ep)) = parse_checkpoint_name(name) else {
            continue;
        };
        if (off, ep) != latest {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Read a specific checkpoint `<end_offset>-<epoch>.checkpoint` by id, or `None`
/// if it is absent (the leader's `FetchSnapshot` serve path).
pub fn load_checkpoint_by_id(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
) -> Option<Vec<u8>> {
    std::fs::read(dir.join(checkpoint_name(end_offset, epoch))).ok()
}
