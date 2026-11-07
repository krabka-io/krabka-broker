//! What a compaction pass does to the stamps of the segments it consumes.
//!
//! The swap set carries no `.stampindex`, so a pass cannot hand the consumed
//! segments' stamp ranges to its output crash-safely. It retires them instead,
//! in memory and on disk alike, as retention and truncation do: the stamps a
//! log reports after a pass are the stamps it reports once reopened.

use {
    super::*,
    crate::{
        config::LogConfig,
        log::test_support::{
            TempLog, append_keyed_samples, compacting_segments, compaction_ctx,
            configured_test_log, install_stamps, set_segment_size,
        },
    },
    assert2::check,
    krabka_units::prelude::{Time, mebibytes, secs},
    std::{fs, path::Path},
};

/// The batches appended: one distinct key each, so a pass keeps every record
/// and only the segment layout changes.
const BATCHES: i64 = 6;

/// The stamp the log reports for every offset it holds.
fn stamps(log: &Log) -> Vec<Option<u64>> {
    (0..BATCHES)
        .map(|offset| log.stamp_for_offset(Offset(offset)))
        .collect()
}

/// The `.stampindex` files in `dir`, by name.
fn stamp_index_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".stampindex"))
        .collect();
    names.sort();
    names
}

#[test]
fn a_pass_retires_the_stamps_of_the_segments_it_consumes() {
    // Both reclamation paths: the immediate unlink of `atomic_swap`, and the
    // delayed retirement to `.deleted` tombstones.
    for (what, delay) in [("no delete delay", Time::ZERO), ("delete delay", secs(60))] {
        let TempLog { dir, mut log } = configured_test_log(LogConfig {
            file_delete_delay: delay,
            ..compacting_segments()
        });
        install_stamps(&mut log, 10, 1);
        append_keyed_samples(&mut log, BATCHES, |i| format!("k{i}"));
        set_segment_size(&log, mebibytes(1));
        log.compact(&compaction_ctx()).unwrap();

        // The five sealed segments collapse into one output at base 0; the
        // active segment at offset 5 keeps its stamp.
        let expected = vec![None, None, None, None, None, Some(15)];
        check!(stamps(&log) == expected, "{what}: after the pass");
        check!(
            stamp_index_files(dir.path()) == vec![format!("{:020}.stampindex", BATCHES - 1)],
            "{what}: on disk after the pass"
        );

        let config = log.config_snapshot();
        drop(log);
        let mut reopened = Log::open(dir.path(), config).unwrap();
        install_stamps(&mut reopened, 100, 1);
        check!(stamps(&reopened) == expected, "{what}: after a reopen");
    }
}
