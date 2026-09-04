//! Crash-safe promotion of the rewritten `.swap` files over the segments they
//! replace. It is the only step of compaction that mutates the live segment
//! file set, so it is kept apart from the passes that only read and write
//! scratch files.

use std::{fs::OpenOptions, path::Path};

use krabka_ids::Offset;
use tracing::instrument;

use super::RewriteOutput;
use crate::{
    error::LogError,
    io::{IoTarget, LogIo},
    name,
};

/// Promote the three `.swap` files that [`rewrite_segments`] produced to final
/// segment files, and delete all consumed sealed segments in between.
///
/// Algorithm (crash-safe):
///   1. `fsync` each `.swap` file.
///   2. For every `consumed_base` in `consumed_base_offsets`,
///      remove `<base>.log`, `<base>.index`, `<base>.timeindex`.
///   3. Rename each `.swap` → final name.
///   4. `fsync` the directory.
///
/// On crash recovery, [`crate::recovery::swap_orphan_recover`] heals
/// any intermediate state.
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
    // Step 1: fsync swap files. Open with write access so
    // `FlushFileBuffers` (Windows) / `fsync` (Linux) succeeds.
    for swap in [
        &rewrite.log_swap,
        &rewrite.index_swap,
        &rewrite.timeindex_swap,
    ] {
        let file = OpenOptions::new().write(true).open(swap)?;
        io.sync_file(IoTarget::CompactionSwap, &file)?;
    }
    if let Some(txn_swap) = &rewrite.txnindex_swap {
        let file = OpenOptions::new().write(true).open(txn_swap)?;
        io.sync_file(IoTarget::CompactionSwap, &file)?;
    }

    // Step 2: delete originals (including consumed `.txnindex` files — the
    // rewritten survivor `.txnindex` carries forward only surviving aborted
    // transactions).
    for base in consumed_base_offsets {
        for path in [
            name::log_path(dir, base.0),
            name::index_path(dir, base.0),
            name::timeindex_path(dir, base.0),
            name::txnindex_path(dir, base.0),
        ] {
            let _ = io.remove_file(IoTarget::CompactionSwap, &path);
        }
    }

    // Step 3: rename swap → final.
    io.rename(
        IoTarget::CompactionSwap,
        &rewrite.log_swap,
        &name::log_path(dir, rewrite.new_base_offset.0),
    )?;
    io.rename(
        IoTarget::CompactionSwap,
        &rewrite.index_swap,
        &name::index_path(dir, rewrite.new_base_offset.0),
    )?;
    io.rename(
        IoTarget::CompactionSwap,
        &rewrite.timeindex_swap,
        &name::timeindex_path(dir, rewrite.new_base_offset.0),
    )?;
    if let Some(txn_swap) = &rewrite.txnindex_swap {
        io.rename(
            IoTarget::CompactionSwap,
            txn_swap,
            &name::txnindex_path(dir, rewrite.new_base_offset.0),
        )?;
    }

    // Step 4: fsync the directory, and report a failure rather than swallow
    // it. Until this returns, the renames above live only in the directory's
    // page cache: a crash here restores the pre-compaction names, and on a
    // compacted topic that is a tombstoned key coming back to life. The
    // caller must treat the swap as unfinished, and
    // [`crate::recovery::swap_orphan_recover`] heals whichever of the two
    // name sets survived on the next `Log::open`.
    io.sync_dir(dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::check;
    use krabka_ids::Offset;
    use krabka_units::prelude::secs;

    use super::*;
    use crate::{
        compact::{
            CleanedTransactionMetadata, RewriteRetention, build_offset_map, rewrite_segments,
            test_support::{make_record, write_sealed_segment},
        },
        io::FileIo,
    };

    #[test]
    fn atomic_swap_replaces_two_segments_with_one() {
        let dir = tempfile::tempdir().unwrap();
        // Build the offset map and rewrite output while segments are open,
        // then drop the segments before atomic_swap so their file handles
        // are closed. On Windows an open file handle prevents rename/delete.
        let rewrite = {
            let first_segment = write_sealed_segment(
                dir.path(),
                0,
                vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            );
            let second_segment = write_sealed_segment(
                dir.path(),
                10,
                vec![make_record(0, Some(b"k1"), Some(b"v2"))],
            );
            let segment_refs = vec![&first_segment, &second_segment];
            let map = build_offset_map(&segment_refs).unwrap();
            let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
            rewrite_segments(
                &FileIo,
                dir.path(),
                &segment_refs,
                &map,
                &txn,
                RewriteRetention {
                    now_ms: 0,
                    delete_retention: secs(1),
                },
                &HashMap::new(),
            )
            .unwrap()
            // first_segment, second_segment dropped here — file handles closed
        };
        atomic_swap(&FileIo, dir.path(), &[Offset(0), Offset(10)], &rewrite).unwrap();

        // After swap: only one .log (base 0). The base 10 segment is gone.
        check!(name::log_path(dir.path(), 0).exists());
        check!(!name::log_path(dir.path(), 10).exists());
        // No leftover .swap files.
        check!(!dir.path().join("00000000000000000000.log.swap").exists());
    }
}
