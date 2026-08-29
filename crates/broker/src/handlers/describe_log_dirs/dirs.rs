//! What the handler reports about a log directory itself: its absolute path,
//! its filesystem capacity, and the result entry an offline directory gets.
//!
//! These three answers are about the directory rather than about the partitions
//! inside it, and two of them are platform-specific, so they sit apart from the
//! scan loop.

use krabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResult;

use crate::codes;

pub(super) fn offline_result(dir: &std::path::Path) -> DescribeLogDirsResult {
    DescribeLogDirsResult {
        error_code: codes::KAFKA_STORAGE_ERROR,
        log_dir: absolute_path(dir),
        topics: Vec::new(),
        total_bytes: -1,
        usable_bytes: -1,
        ..Default::default()
    }
}

/// Best-effort absolute path string for a log dir.
///
/// The result matches the "absolute log directory path" contract of Kafka. The
/// function falls back to the lexical path when the canonicalization fails, for
/// example after another process removes the dir.
pub(super) fn absolute_path(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap_or_else(|_| dir.to_path_buf())
        .display()
        .to_string()
}

/// `(total_bytes, usable_bytes)` for the filesystem that hosts `dir`.
///
/// The pair matches the KIP-827 `DescribeLogDirsResult` v4 fields.
/// `total_bytes` is the capacity of the filesystem. `usable_bytes` is the space
/// available to a non-root caller, so it respects the typical 5 % root reserve.
///
/// Returns `(-1, -1)`, the Kafka "unknown" sentinel, when the platform has no
/// `statvfs`, as on Windows, or when the syscall fails. The syscall fails when
/// the path vanishes during a reconfigure, or on a permission error. The JVM
/// admin tools tolerate `-1` and skip the column.
pub(super) fn log_dir_capacity(dir: &std::path::Path) -> (i64, i64) {
    disk_stats(dir).unwrap_or((-1, -1))
}

#[cfg(unix)]
fn disk_stats(dir: &std::path::Path) -> Option<(i64, i64)> {
    let stat = rustix::fs::statvfs(dir).ok()?;
    // `f_frsize` is the fragment size in bytes; multiplying by the
    // block counts yields capacity in bytes. Both fields come back as
    // `u64`; clamp to `i64::MAX` rather than overflow on a hypothetical
    // exabyte-scale volume.
    let frsize = i64::try_from(stat.f_frsize).unwrap_or(i64::MAX);
    let total = i64::try_from(stat.f_blocks)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    let usable = i64::try_from(stat.f_bavail)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    Some((total, usable))
}

#[cfg(not(unix))]
fn disk_stats(_dir: &std::path::Path) -> Option<(i64, i64)> {
    None
}

// Both tests below are unix-only, so the module itself is gated: on another
// platform there would be no test left to use the `assert2` imports.
#[cfg(all(test, unix))]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// On unix, `statvfs` against any tempdir must return sensible positive
    /// numbers.
    ///
    /// The rule is `total_bytes >= usable_bytes > 0`. This test catches a
    /// regression in the multiplication of the fragment size by the block
    /// count. Such a regression reports zeros silently. The Kafka tools then
    /// display "0 B free", and operators chase a problem that does not exist.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_sensible_unix_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let (total, usable) = log_dir_capacity(tmp.path());
        check!(
            total > 0,
            "total_bytes must be positive on unix tempdir, got {total}"
        );
        check!(
            usable > 0,
            "usable_bytes must be positive on unix tempdir, got {usable}"
        );
        check!(
            total >= usable,
            "total_bytes ({total}) must be ≥ usable_bytes ({usable})",
        );
    }

    /// A vanished path gives the Kafka "unknown" sentinel and not the syscall
    /// error.
    ///
    /// Operators see `-1`, and the JVM tool skips the column. The alternative
    /// is a `KafkaStorageException`, much like a 500, which would block the
    /// whole describe.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_minus_one_for_missing_path() {
        let phantom = std::path::Path::new("/nonexistent/krabka/test/dir/should/not/exist");
        assert!(log_dir_capacity(phantom) == (-1, -1));
    }
}
