//! Names inside a Kafka log directory: the per-partition directory, and the segment files in it.
//!
//! A partition's data lives in `<log_dir>/<topic>-<partition>/`. Kafka names each segment in that directory by its 20-digit zero-padded base offset, with the `.log`, `.index`, and `.timeindex` extensions.
//!
//! The module is public because a tool that opens a log directory needs the same paths the broker uses, and it must not have to link the broker to compute them.

use {
    crate::error::LogError,
    std::path::{Path, PathBuf},
};

/// Digit count of a segment filename stem. Kafka zero-pads the base offset to this width.
pub const FILENAME_DIGITS: usize = 20;

/// `0` → `"00000000000000000000"`. `1847` → `"00000000000000001847"`.
#[must_use]
pub fn format_base_offset(base_offset: i64) -> String { format!("{base_offset:020}") }

/// Parse a `.log` filename and return its base offset.
/// `"00000000000000001847.log"` → `Ok(1847)`.
///
/// # Errors
///
/// Returns [`LogError::BadSegmentName`] when the name has no `.log` extension, when the stem is not [`FILENAME_DIGITS`] characters long, or when the stem is not a decimal `i64`.
pub fn parse_log_filename(name: &str) -> Result<i64, LogError> {
    let stem = name
        .strip_suffix(".log")
        .ok_or_else(|| LogError::BadSegmentName(name.into()))?;
    if stem.len() != FILENAME_DIGITS {
        return Err(LogError::BadSegmentName(name.into()));
    }
    stem.parse::<i64>()
        .map_err(|_| LogError::BadSegmentName(name.into()))
}

/// A file that a segment keeps under its base-offset stem, or the producer
/// snapshot that sits at the same stem. Each kind has one on-disk extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentFile {
    /// `.log`: the record batches.
    Log,
    /// `.index`: the sparse offset index.
    Index,
    /// `.timeindex`: the sparse timestamp index.
    TimeIndex,
    /// `.txnindex`: the aborted-transaction index. A segment with no aborted
    /// transaction has none.
    TxnIndex,
    /// `.stampindex`: the internal stamp coordinate. A partition that stamps
    /// nothing has none.
    StampIndex,
    /// `.snapshot`: the producer state as of the stem's offset.
    Snapshot,
}

impl SegmentFile {
    /// What retention retires with a deleted segment: [`Self::SEGMENT`] and
    /// the producer snapshot at the segment's base offset.
    pub const RETIRED_WITH_SEGMENT: [Self; 6] = [
        Self::Log,
        Self::Index,
        Self::TimeIndex,
        Self::TxnIndex,
        Self::StampIndex,
        Self::Snapshot,
    ];
    /// Every file a segment owns: its `.log` and its sidecars. A truncation
    /// and a completed compaction swap remove these for a dropped segment.
    pub const SEGMENT: [Self; 5] = [
        Self::Log,
        Self::Index,
        Self::TimeIndex,
        Self::TxnIndex,
        Self::StampIndex,
    ];
    /// The sidecars a segment can carry beside its `.log`.
    pub const SIDECARS: [Self; 4] = [
        Self::Index,
        Self::TimeIndex,
        Self::TxnIndex,
        Self::StampIndex,
    ];
    /// The files of a compaction swap set, the `.log` first. A swap carries
    /// no `.stampindex`, so the survivor takes no stamp ranges over.
    pub const SWAP_SET: [Self; 4] = [Self::Log, Self::Index, Self::TimeIndex, Self::TxnIndex];
    /// The sidecars of a compaction swap set.
    pub const SWAP_SIDECARS: [Self; 3] = [Self::Index, Self::TimeIndex, Self::TxnIndex];

    /// The file's extension, without the dot.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Log => "log",
            Self::Index => "index",
            Self::TimeIndex => "timeindex",
            Self::TxnIndex => "txnindex",
            Self::StampIndex => "stampindex",
            Self::Snapshot => "snapshot",
        }
    }

    /// The extensions of `files`, in order.
    #[must_use]
    pub const fn extensions<const N: usize>(files: [Self; N]) -> [&'static str; N] {
        let mut extensions = [""; N];
        let mut index = 0;
        while index < N {
            extensions[index] = files[index].extension();
            index += 1;
        }
        extensions
    }

    /// The path of this file for the stem `base_offset` in `dir`.
    #[must_use]
    pub fn path(self, dir: &Path, base_offset: i64) -> PathBuf {
        dir.join(format!(
            "{}.{}",
            format_base_offset(base_offset),
            self.extension()
        ))
    }
}

/// Path to the `.log` file of the segment that starts at `base_offset`. It holds the record batches.
#[must_use]
pub fn log_path(dir: &Path, base_offset: i64) -> PathBuf { SegmentFile::Log.path(dir, base_offset) }

/// Path to the sparse offset index, `.index`, of the segment that starts at `base_offset`.
#[must_use]
pub fn index_path(dir: &Path, base_offset: i64) -> PathBuf {
    SegmentFile::Index.path(dir, base_offset)
}

/// Path to the sparse timestamp index, `.timeindex`, of the segment that starts at `base_offset`.
#[must_use]
pub fn timeindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    SegmentFile::TimeIndex.path(dir, base_offset)
}

/// Path to the aborted-transaction index, `.txnindex`, of the segment that starts at `base_offset`.
#[must_use]
pub fn txnindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    SegmentFile::TxnIndex.path(dir, base_offset)
}

/// Path to the `.snapshot` file that holds the producer state as of `offset`.
#[must_use]
pub fn producer_snapshot_path(dir: &Path, offset: i64) -> PathBuf {
    SegmentFile::Snapshot.path(dir, offset)
}

/// Path to the per-segment `.stampindex` sidecar. It holds the additional
/// internal stamp coordinate and is never a client-facing file.
#[must_use]
pub fn stampindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    SegmentFile::StampIndex.path(dir, base_offset)
}

/// Path to the per-partition `.leader-epoch-checkpoint` file.
#[must_use]
pub fn leader_epoch_checkpoint_path(dir: &Path) -> PathBuf { dir.join("leader-epoch-checkpoint") }

/// Path to the per-partition `log-start-offset-checkpoint` file.
///
/// It holds the log start offset that no segment name records: the part of a
/// trim that lands inside a segment rather than on a segment boundary.
#[must_use]
pub fn log_start_offset_checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("log-start-offset-checkpoint")
}

/// Suffix that the broker appends to a future-log partition directory while a KIP-113 intra-broker move runs.
///
/// The directory at `<target_log_dir>/<topic>-<partition><FUTURE_SUFFIX>` collects copied batches. When the future log catches up, the broker renames it in place to `<topic>-<partition>`.
///
/// This is not the name that Apache Kafka gives the directory. Kafka writes `<topic>-<partition>.<32-hex-uuid>-future` (`LocalLog.logFutureDirName`, `LogFileUtils.FUTURE_DIR_SUFFIX`), and a JVM broker cannot parse the name without the unique id. A partition has at most one move, and only the krabka broker reads the name, so the unique id is left out. `docs/format-divergences.md` lists the difference.
pub const FUTURE_SUFFIX: &str = "-future";

/// Builds the directory path for a (topic, partition).
#[must_use]
pub fn partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}"))
}

/// Parses `<topic>-<partition>` from a directory name. It returns `None` when the name does not match the pattern.
#[must_use]
pub fn parse_partition_dir(name: &str) -> Option<(String, i32)> {
    let (topic, part) = name.rsplit_once('-')?;
    if topic.is_empty() || topic.ends_with('-') {
        // Empty topic or trailing `-` in the topic indicates a malformed
        // name like "-0" or "foo--1" (which would otherwise parse the
        // tail as a positive partition number).
        return None;
    }
    let partition = part.parse::<i32>().ok()?;
    if partition < 0 {
        return None;
    }
    Some((topic.to_owned(), partition))
}

#[cfg(test)]
mod tests {

    use {super::*, assert2::assert};

    fn check_offset_case(offset: i64, expected_filename: &str) {
        let formatted = format_base_offset(offset);
        assert!(formatted == expected_filename);
        let parsed = parse_log_filename(&format!("{formatted}.log")).unwrap();
        assert!(parsed == offset);
    }

    #[test]
    fn zero() { check_offset_case(0, "00000000000000000000"); }
    #[test]
    fn small() { check_offset_case(1847, "00000000000000001847"); }
    // Plan had "00000000001000000000000" (23 chars) which is wrong:
    // `{:020}` zero-pads to 20 chars, and 1_000_000_000_000 is 13 digits,
    // so we expect 7 leading zeros + "1000000000000" = 20 chars total.
    #[test]
    fn large() { check_offset_case(1_000_000_000_000, "00000001000000000000"); }

    #[test]
    fn rejects_non_log_extension() {
        assert2::assert!(parse_log_filename("00000000000000000000.index").is_err());
    }

    #[yare::parameterized(
        too_few_digits = { "123.log" },
        too_many_digits = { "000000000000000001847.log" },
    )]
    fn rejects_wrong_digit_count(filename: &str) {
        assert2::assert!(parse_log_filename(filename).is_err());
    }

    #[test]
    fn round_trip_partition_dir() {
        let p = partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert!(parse_partition_dir(name) == Some(("foo".to_owned(), 7)));
        assert!(parse_partition_dir("foo-0") == Some(("foo".to_owned(), 0)));
    }

    #[test]
    fn rejects_negative_partition() {
        assert!(parse_partition_dir("foo--1") == None);
    }

    #[test]
    fn rejects_empty_topic() {
        assert!(parse_partition_dir("-0") == None);
    }

    #[test]
    fn rejects_no_dash() {
        assert!(parse_partition_dir("foo") == None);
    }

    #[test]
    fn handles_topic_with_dashes() {
        // Topic names can themselves contain hyphens; rsplit takes the last.
        assert!(parse_partition_dir("my-cool-topic-3") == Some(("my-cool-topic".to_owned(), 3)));
    }

    /// Every named file set, against the extensions written out by hand: a
    /// set that gained or lost a file would delete or keep the wrong files.
    #[test]
    fn segment_file_sets_hold_their_on_disk_extensions() {
        let sets: [(&str, Vec<&str>, &[&str]); 5] = [
            (
                "sidecars",
                SegmentFile::extensions(SegmentFile::SIDECARS).to_vec(),
                &["index", "timeindex", "txnindex", "stampindex"],
            ),
            (
                "segment",
                SegmentFile::extensions(SegmentFile::SEGMENT).to_vec(),
                &["log", "index", "timeindex", "txnindex", "stampindex"],
            ),
            (
                "retired with a segment",
                SegmentFile::extensions(SegmentFile::RETIRED_WITH_SEGMENT).to_vec(),
                &[
                    "log",
                    "index",
                    "timeindex",
                    "txnindex",
                    "stampindex",
                    "snapshot",
                ],
            ),
            (
                "swap set",
                SegmentFile::extensions(SegmentFile::SWAP_SET).to_vec(),
                &["log", "index", "timeindex", "txnindex"],
            ),
            (
                "swap sidecars",
                SegmentFile::extensions(SegmentFile::SWAP_SIDECARS).to_vec(),
                &["index", "timeindex", "txnindex"],
            ),
        ];
        for (name, got, want) in sets {
            assert!(got == want, "{name}");
        }
    }

    /// Each file kind names the file Kafka and earlier krabka releases wrote.
    #[test]
    fn segment_file_paths_keep_their_on_disk_names() {
        let dir = Path::new("/data/t-0");
        let cases = [
            (
                SegmentFile::Log,
                log_path(dir, 7),
                "00000000000000000007.log",
            ),
            (
                SegmentFile::Index,
                index_path(dir, 7),
                "00000000000000000007.index",
            ),
            (
                SegmentFile::TimeIndex,
                timeindex_path(dir, 7),
                "00000000000000000007.timeindex",
            ),
            (
                SegmentFile::TxnIndex,
                txnindex_path(dir, 7),
                "00000000000000000007.txnindex",
            ),
            (
                SegmentFile::StampIndex,
                stampindex_path(dir, 7),
                "00000000000000000007.stampindex",
            ),
            (
                SegmentFile::Snapshot,
                producer_snapshot_path(dir, 7),
                "00000000000000000007.snapshot",
            ),
        ];
        for (file, helper_path, want) in cases {
            assert!(file.path(dir, 7) == dir.join(want), "{file:?}");
            assert!(helper_path == dir.join(want), "{file:?}");
        }
    }
}
