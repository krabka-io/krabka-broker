//! Open-time recovery for log directories.
//!
//! - `Segment::open_active` handles partial trailing batches in the
//!   active segment.
//! - [`swap_orphan_recover`] handles `.swap` files that an interrupted
//!   [`crate::compact::atomic_swap`] left behind.
//! - [`deleted_orphan_recover`] handles `.deleted` tombstones and stranded
//!   sidecars that an interrupted
//!   [`crate::retention::delete_segment_files`] left behind.

use std::{collections::HashSet, path::Path};

use tracing::instrument;

use crate::{
    error::LogError,
    io::{IoTarget, LogIo},
    name,
    retention::DELETED_SUFFIX,
};

/// Heal any canonical `<base>.<sidecar>.swap` set found in `dir`:
///
/// - If the matching plain `<base>.log` exists, the swap was in step 1 or 2
///   and the originals are still authoritative, so delete the swap triple.
/// - If the final log is absent and its swap exists, promotion has not started;
///   promote the log and every surviving sidecar.
/// - If the final log exists but its swap is absent, the log rename completed;
///   continue promoting sidecars. Reject sidecars with neither form of log.
///
/// This function is idempotent. It is safe to call on every `Log::open`.
#[instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), swaps = tracing::field::Empty),
    err,
)]
pub fn swap_orphan_recover(dir: &Path) -> Result<(), LogError> {
    let entries = std::fs::read_dir(dir)?;
    let mut swap_bases: HashSet<i64> = HashSet::new();
    let mut log_swap_bases: HashSet<i64> = HashSet::new();
    let mut existing_log_bases: HashSet<i64> = HashSet::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Some((base, is_log)) = parse_swap_filename(name) {
            swap_bases.insert(base);
            if is_log {
                log_swap_bases.insert(base);
            }
        }
        if let Ok(base) = name::parse_log_filename(name) {
            existing_log_bases.insert(base);
        }
    }

    let mut swap_bases: Vec<i64> = swap_bases.into_iter().collect();
    swap_bases.sort_unstable();
    tracing::Span::current().record("swaps", swap_bases.len());
    for base in swap_bases {
        let log_swap = swap_triple(dir, base, "log");
        let index_swap = swap_triple(dir, base, "index");
        let timeindex_swap = swap_triple(dir, base, "timeindex");
        let txnindex_swap = swap_triple(dir, base, "txnindex");

        match krabka_verified::local_recovery_swap_action(
            existing_log_bases.contains(&base),
            log_swap_bases.contains(&base),
        ) {
            krabka_verified::LocalRecoverySwapAction::DiscardSwap => {
                // Orphan partial — discard. The `.txnindex.swap` is only
                // produced when the survivor segment retains aborted txns
                // (`RewriteOutput::txnindex_swap` is `Option`), so it may be
                // absent; `remove_file` no-ops on a missing path.
                let _ = std::fs::remove_file(&log_swap);
                let _ = std::fs::remove_file(&index_swap);
                let _ = std::fs::remove_file(&timeindex_swap);
                let _ = std::fs::remove_file(&txnindex_swap);
            }
            krabka_verified::LocalRecoverySwapAction::PromoteAll => {
                // Complete swap interrupted mid-rename — promote.
                std::fs::rename(&log_swap, name::log_path(dir, base))?;
                // The index / timeindex .swap files may not exist if the
                // crash happened *between* the three renames. Tolerate
                // missing sidecars — `Segment::open` accepts empty index files
                // and rebuilds on tail-scan.
                if index_swap.exists() {
                    std::fs::rename(&index_swap, name::index_path(dir, base))?;
                } else {
                    std::fs::File::create(name::index_path(dir, base))?;
                }
                if timeindex_swap.exists() {
                    std::fs::rename(&timeindex_swap, name::timeindex_path(dir, base))?;
                } else {
                    std::fs::File::create(name::timeindex_path(dir, base))?;
                }
                // The `.txnindex` is an OPTIONAL sidecar (`atomic_swap` only
                // renames it when the survivor retained aborted txns). Unlike
                // the index / timeindex, a segment with no aborted txns has NO
                // `.txnindex` at all and `Segment::open` tolerates its absence,
                // so we must NOT synthesize an empty one. Promote the survivor
                // `.txnindex.swap` if present; otherwise remove any leftover
                // `.txnindex` at the target offset so a stale pre-swap index
                // can't outlive the segment it described.
                if txnindex_swap.exists() {
                    std::fs::rename(&txnindex_swap, name::txnindex_path(dir, base))?;
                } else {
                    let _ = std::fs::remove_file(name::txnindex_path(dir, base));
                }
            }
            krabka_verified::LocalRecoverySwapAction::PromoteSidecars => {
                // The log rename completed before interruption. Continue the
                // remaining sidecars without discarding a transaction index.
                promote_or_create(&index_swap, &name::index_path(dir, base))?;
                promote_or_create(&timeindex_swap, &name::timeindex_path(dir, base))?;
                if txnindex_swap.exists() {
                    std::fs::rename(&txnindex_swap, name::txnindex_path(dir, base))?;
                }
            }
            krabka_verified::LocalRecoverySwapAction::Reject => {
                return Err(LogError::Corrupt(format!(
                    "swap sidecars for segment {base} have no log file"
                )));
            }
        }
    }
    Ok(())
}

/// The sidecar extensions a segment carries beside its `.log`.
const SIDECAR_EXTENSIONS: [&str; 4] = ["index", "timeindex", "txnindex", "stampindex"];

/// Reclaim what an interrupted segment deletion left in `dir`.
///
/// [`crate::retention::delete_segment_files`] renames every file of a segment
/// to a `<name>.deleted` tombstone before it unlinks any of them, so a failure
/// part-way through leaves one of two things behind:
///
/// - a tombstone nothing reads, which is unlinked here; or
/// - a sidecar whose `.log` is already gone. [`crate::Log::open`] enumerates
///   segments by `.log` filename, so such a sidecar is invisible to every
///   later pass and is never reclaimed. It is unlinked here too.
///
/// This function is idempotent, and is safe to call on every `Log::open`. It
/// runs after [`swap_orphan_recover`], which is what promotes a `.log.swap`
/// into the `.log` that keeps its sidecars alive.
///
/// # Errors
/// Returns an error when the directory scan or an unlink fails.
#[instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), reclaimed = tracing::field::Empty),
    err,
)]
pub fn deleted_orphan_recover(io: &dyn LogIo, dir: &Path) -> Result<(), LogError> {
    let mut log_bases: HashSet<i64> = HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Ok(base) = name::parse_log_filename(name) {
            log_bases.insert(base);
        }
        names.push(name.to_owned());
    }

    let mut reclaimed = 0usize;
    for name in names {
        if !is_deleted_tombstone(&name) && !is_stranded_sidecar(&name, &log_bases) {
            continue;
        }
        match io.remove_file(IoTarget::SegmentDeletion, &dir.join(&name)) {
            Ok(()) => reclaimed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(LogError::Io(error)),
        }
    }
    tracing::Span::current().record("reclaimed", reclaimed);
    Ok(())
}

/// `true` for `<20 digits>.<ext>.deleted`, the tombstone retention renames to.
fn is_deleted_tombstone(value: &str) -> bool {
    let Some(rest) = value.strip_suffix(DELETED_SUFFIX) else {
        return false;
    };
    let Some(rest) = rest.strip_suffix('.') else {
        return false;
    };
    ["log"]
        .into_iter()
        .chain(SIDECAR_EXTENSIONS)
        .any(|ext| canonical_base(rest, ext).is_some())
}

/// `true` for a canonical sidecar name whose segment `.log` is not present.
fn is_stranded_sidecar(value: &str, log_bases: &HashSet<i64>) -> bool {
    SIDECAR_EXTENSIONS
        .into_iter()
        .filter_map(|ext| canonical_base(value, ext))
        .any(|base| !log_bases.contains(&base))
}

/// The base offset `value` names when it is exactly `<20 digits>.<ext>`.
fn canonical_base(value: &str, ext: &str) -> Option<i64> {
    let stem = value.strip_suffix(ext)?.strip_suffix('.')?;
    if stem.len() != name::FILENAME_DIGITS {
        return None;
    }
    let base = stem.parse::<i64>().ok()?;
    (base >= 0 && name::format_base_offset(base) == stem).then_some(base)
}

fn parse_swap_filename(value: &str) -> Option<(i64, bool)> {
    for (suffix, is_log) in [
        (".log.swap", true),
        (".index.swap", false),
        (".timeindex.swap", false),
        (".txnindex.swap", false),
    ] {
        let Some(stem) = value.strip_suffix(suffix) else {
            continue;
        };
        if stem.len() != name::FILENAME_DIGITS {
            return None;
        }
        let base = stem.parse::<i64>().ok()?;
        return (base >= 0 && name::format_base_offset(base) == stem).then_some((base, is_log));
    }
    None
}

fn promote_or_create(swap: &Path, target: &Path) -> Result<(), LogError> {
    if swap.exists() {
        std::fs::rename(swap, target)?;
    } else if !target.exists() {
        std::fs::File::create(target)?;
    }
    Ok(())
}

fn swap_triple(dir: &Path, base: i64, ext: &str) -> std::path::PathBuf {
    dir.join(format!("{}.{}.swap", name::format_base_offset(base), ext))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    fn touch(path: &std::path::Path) {
        std::fs::File::create(path).unwrap();
    }

    #[test]
    fn discards_swap_when_original_log_still_present() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        touch(&name::log_path(p, 0));
        touch(&name::index_path(p, 0));
        touch(&name::timeindex_path(p, 0));
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        touch(&p.join("00000000000000000000.txnindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(!p.join("00000000000000000000.log.swap").exists());
        check!(!p.join("00000000000000000000.index.swap").exists());
        check!(!p.join("00000000000000000000.timeindex.swap").exists());
        // The orphaned survivor `.txnindex.swap` is discarded too.
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }

    #[test]
    fn promotes_swap_when_original_log_missing() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // No originals — only .swap triples (= post-step-2, pre-step-3).
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::index_path(p, 0).exists());
        check!(name::timeindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.log.swap").exists());
    }

    #[test]
    fn promotes_txnindex_swap_when_present() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // Complete swap (originals gone) whose survivor retained aborted
        // txns, so a `.txnindex.swap` exists alongside the other three.
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        touch(&p.join("00000000000000000000.txnindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::txnindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.txnindex.swap").exists());

        swap_orphan_recover(p).unwrap();
        check!(name::txnindex_path(p, 0).exists());
    }

    #[test]
    fn continues_sidecar_promotion_after_log_rename() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        touch(&name::log_path(p, 0));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        touch(&p.join("00000000000000000000.txnindex.swap"));

        swap_orphan_recover(p).unwrap();

        check!(name::log_path(p, 0).exists());
        check!(name::index_path(p, 0).exists());
        check!(name::timeindex_path(p, 0).exists());
        check!(name::txnindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }

    #[test]
    fn promote_without_txnindex_swap_synthesizes_none_and_clears_stale() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // A stale `.txnindex` from a prior segment sits at the target
        // offset, but the survivor of THIS swap had no aborted txns, so
        // no `.txnindex.swap` was produced. Recovery must NOT synthesize
        // an empty `.txnindex` and must clear the stale one so it can't
        // outlive the segment it described.
        touch(&name::txnindex_path(p, 0));
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        // No `.txnindex` is synthesized when the survivor had none.
        check!(!name::txnindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }

    /// The sweep reclaims both shapes an interrupted deletion leaves: a
    /// `.deleted` tombstone, and a sidecar whose `.log` is already gone.
    #[test]
    fn deleted_orphan_recover_reclaims_tombstones_and_stranded_sidecars() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // A live segment at base 0, whose sidecars must survive untouched.
        touch(&name::log_path(p, 0));
        touch(&name::index_path(p, 0));
        touch(&name::timeindex_path(p, 0));
        touch(&name::stampindex_path(p, 0));
        // Tombstones from a deletion that renamed but never unlinked.
        let tombstones = [
            p.join("00000000000000000010.log.deleted"),
            p.join("00000000000000000010.index.deleted"),
            p.join("00000000000000000010.timeindex.deleted"),
        ];
        for path in &tombstones {
            touch(path);
        }
        // Sidecars of a segment whose `.log` did get unlinked. The `.log`-keyed
        // scan cannot see these, so nothing else would ever reclaim them.
        let stranded = [
            name::index_path(p, 20),
            name::timeindex_path(p, 20),
            name::txnindex_path(p, 20),
            name::stampindex_path(p, 20),
        ];
        for path in &stranded {
            touch(path);
        }
        // Files the sweep has no business touching.
        let unrelated = [
            p.join("leader-epoch-checkpoint"),
            p.join("00000000000000000000.snapshot"),
            p.join("not-a-segment.index"),
        ];
        for path in &unrelated {
            touch(path);
        }

        deleted_orphan_recover(&crate::io::FileIo, p).unwrap();

        for path in tombstones.iter().chain(stranded.iter()) {
            check!(!path.exists(), "reclaimed: {}", path.display());
        }
        for path in unrelated.iter().chain(
            [
                name::log_path(p, 0),
                name::index_path(p, 0),
                name::timeindex_path(p, 0),
                name::stampindex_path(p, 0),
            ]
            .iter(),
        ) {
            check!(path.exists(), "left alone: {}", path.display());
        }

        // Idempotent: a second sweep over the healed directory changes nothing.
        deleted_orphan_recover(&crate::io::FileIo, p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::index_path(p, 0).exists());
    }

    #[test]
    fn deleted_orphan_recover_reports_a_directory_scan_failure() {
        let dir = tempdir().unwrap();

        check!(matches!(
            deleted_orphan_recover(&crate::io::FileIo, &dir.path().join("missing")),
            Err(LogError::Io(_))
        ));
    }

    #[test]
    fn ignores_malformed_swap_names() {
        let dir = tempdir().unwrap();
        let malformed = dir.path().join("not-a-segment.log.swap");
        touch(&malformed);

        swap_orphan_recover(dir.path()).unwrap();

        check!(malformed.exists());
    }

    #[test]
    fn reports_directory_scan_failure() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("missing");

        check!(matches!(
            swap_orphan_recover(&missing),
            Err(LogError::Io(_))
        ));
    }

    #[test]
    fn rejects_sidecars_without_any_log() {
        let dir = tempdir().unwrap();
        touch(&dir.path().join("00000000000000000000.index.swap"));

        check!(matches!(
            swap_orphan_recover(dir.path()),
            Err(LogError::Corrupt(message)) if message.contains("no log file")
        ));
    }
}
