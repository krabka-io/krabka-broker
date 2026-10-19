//! Crash-safe promotion of a compaction's survivor segment over the segments
//! it replaces. It is the only step of compaction that mutates the live
//! segment file set, so it is kept apart from the passes that only read and
//! write scratch files.
//!
//! The protocol, and the recovery that finishes it after a crash, are
//! described in [`crate::recovery::swap`].

use {
    super::RewriteOutput,
    crate::{
        error::LogError,
        io::{IoTarget, LogIo},
        name::producer_snapshot_path,
        recovery::swap::{
            cleaned_path, promote_log, promote_sidecars, remove_if_present,
            remove_replaced_segment, swap_path,
        },
    },
    krabka_ids::Offset,
    std::{fs::OpenOptions, path::Path},
    tracing::instrument,
};

/// Promote the survivor files that [`rewrite_segments`] produced to final
/// segment files, and delete every consumed sealed segment, as Kafka's
/// `LocalLog.replaceSegments` does:
///
///   1. `fsync` each survivor file.
///   2. Rename the sidecars to `.swap`, `fsync` the directory, then rename the
///      log to `.log.swap` and `fsync` the directory. The durable `.log.swap`
///      commits the swap.
///   3. Delete every file of every consumed segment, and `fsync` the
///      directory. A failed unlink fails the swap.
///   4. Rename the log to its final name and `fsync` the directory, then the
///      sidecars, and `fsync` the directory.
///
/// A crash before the commit leaves every consumed segment intact, and
/// [`crate::recovery::swap_orphan_recover`] aborts the swap. A crash after it
/// may leave the swap as the only copy of some records, and recovery
/// completes it.
///
/// [`rewrite_segments`]: super::rewrite_segments
///
/// # Errors
/// Returns the first file, rename, unlink or directory-fsync failure. The swap
/// is then unfinished; the next `Log::open` completes or aborts it.
#[instrument(
    level = "info",
    skip_all,
    fields(
        dir = %dir.display(),
        consumed = consumed_base_offsets.len(),
        new_base = rewrite.new_base_offset.0,
    ),
    err,
)]
pub fn atomic_swap(
    io: &dyn LogIo,
    dir: &Path,
    consumed_base_offsets: &[Offset],
    rewrite: &RewriteOutput,
) -> Result<(), LogError> {
    atomic_swap_retiring(io, dir, consumed_base_offsets, rewrite, |base| {
        remove_replaced_segment(io, dir, base.0)?;
        if replaces_producer_snapshot(base, rewrite.new_base_offset) {
            remove_if_present(io, &producer_snapshot_path(dir, base.0))?;
        }
        Ok(())
    })
}

/// Whether a swap deletes the producer-state snapshot of the consumed segment
/// at `consumed`.
///
/// Kafka's `UnifiedLog.replaceSegments` deletes the snapshot of every consumed
/// segment that no survivor takes over, through `deleteProducerSnapshots` on
/// the segments `LocalLog.replaceSegments` returns. A snapshot at the
/// survivor's own base offset stays, because the survivor starts there. Without
/// this, every compaction pass leaves the snapshot of each segment it merged,
/// and a compacted partition gains one snapshot file per roll until it is
/// reopened.
#[must_use]
pub(crate) fn replaces_producer_snapshot(consumed: Offset, new_base: Offset) -> bool {
    consumed != new_base
}

/// The swap protocol with caller-owned reclamation of replaced files.
pub(crate) fn atomic_swap_retiring(
    io: &dyn LogIo,
    dir: &Path,
    consumed_base_offsets: &[Offset],
    rewrite: &RewriteOutput,
    mut retire: impl FnMut(Offset) -> Result<(), LogError>,
) -> Result<(), LogError> {
    let base = rewrite.new_base_offset.0;
    let sidecars = [
        (Some(&rewrite.index_swap), "index"),
        (Some(&rewrite.timeindex_swap), "timeindex"),
        (rewrite.txnindex_swap.as_ref(), "txnindex"),
    ];

    // Step 1: fsync the survivor files. Open with write access so
    // `FlushFileBuffers` (Windows) / `fsync` (Linux) succeeds.
    for path in [Some(&rewrite.log_swap)]
        .into_iter()
        .chain(sidecars.map(|(path, _)| path))
        .flatten()
    {
        let file = OpenOptions::new().write(true).open(path)?;
        io.sync_file(IoTarget::CompactionSwap, &file)?;
    }

    // Step 2: stage the sidecars, then commit with the log. A survivor with no
    // aborted transaction has no `.txnindex`, so any stale one a failed
    // earlier pass left at this base must not be promoted with it.
    for (path, ext) in sidecars {
        if let Some(path) = path {
            stage(io, path, &swap_path(dir, base, ext))?;
        } else {
            remove_if_present(io, &swap_path(dir, base, ext))?;
            remove_if_present(io, &cleaned_path(dir, base, ext))?;
        }
    }
    io.sync_dir(dir)?;
    stage(io, &rewrite.log_swap, &swap_path(dir, base, "log"))?;
    io.sync_dir(dir)?;

    // Step 3: delete the consumed segments. Only now may they go, and they
    // must be gone for good before the final rename can make the survivor
    // overlap one of them.
    for consumed in consumed_base_offsets {
        retire(*consumed)?;
    }
    io.sync_dir(dir)?;

    // Step 4: rename the swap set onto its final names.
    promote_log(io, dir, base)?;
    promote_sidecars(io, dir, base)
}

/// Rename a survivor file from where the rewrite wrote it to its `.swap` name.
fn stage(io: &dyn LogIo, from: &Path, to: &Path) -> Result<(), LogError> {
    if from != to {
        io.rename(IoTarget::CompactionSwap, from, to)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
