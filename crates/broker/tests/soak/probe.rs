//! Reading a resource level off a running broker, and off its log directory.
//!
//! Nothing in this workspace measured a resource level before this suite: a
//! search for `getrusage`, `VmRSS` or `/proc/self/fd` across `crates/` found
//! nothing, and `docs/operations/capacity.md` tells the operator to size file
//! descriptors as "one per client connection plus the segment files of every
//! replica" while `krabka_broker_active_connections` counts only the first
//! half. These are the numbers that make that sizing checkable:
//!
//! | sample | where it comes from | what a rise would mean |
//! | --- | --- | --- |
//! | resident set | `/proc/<pid>/status` `VmRSS` | a leak: producer-id state, fetch sessions, the diskless hot tail |
//! | open descriptors | entries in `/proc/<pid>/fd` | segment handles or connections not being closed |
//! | `/metrics` series | sample lines in the scraped body | an unbounded metric family -- a per-partition or per-client label set that never drops a member |
//! | log-directory bytes and files | the host side of the bind mount, per top-level entry | retention, the cleaner or the metadata log not reclaiming what they replace |
//!
//! The pid comes from `docker inspect` and the container runs as the host user
//! that owns its data directory, so both `/proc` reads work from the test
//! process without a shell inside the image. The parsing is separated from the
//! IO here because the parsing is what can be wrong quietly: a `VmRSS` line
//! read in the wrong unit, or a `/metrics` count that included the `# HELP`
//! lines, would move every number this lane judges.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// A sampled count as a float, for the trend analysis.
///
/// Every reading this suite takes is bounded by a ceiling well inside `u32` --
/// the largest is a 1.5 GiB resident set -- so the conversion is exact. A
/// reading that does not fit saturates, and `u32::MAX` is above every ceiling
/// here, so an implausible sample fails its series rather than wrapping into a
/// passing one.
pub(crate) fn sampled(value: u64) -> f64 { f64::from(u32::try_from(value).unwrap_or(u32::MAX)) }

/// The same, for the `usize` a count comes back as.
pub(crate) fn sampled_count(value: usize) -> f64 {
    sampled(u64::try_from(value).unwrap_or(u64::MAX))
}

/// Resident set size in bytes, from a `/proc/<pid>/status` body.
///
/// `VmRSS` is reported in kibibytes, always: the kernel writes the unit on the
/// line and it has been `kB` for the whole life of the file. Returns `None`
/// when the process has exited between `docker inspect` and this read, which
/// leaves an unreadable or truncated file rather than an error.
pub(crate) fn parse_vm_rss_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let value: u64 = fields.next()?.parse().ok()?;
    let unit = fields.next()?;
    let scale = match unit {
        "kB" => 1024,
        "mB" | "MB" => 1024 * 1024,
        _ => return None,
    };
    Some(value * scale)
}

/// Sample lines in a Prometheus exposition body.
///
/// One line per series-and-labels, so this is the cardinality a scrape carries.
/// `# HELP` and `# TYPE` are metadata about a family rather than members of it,
/// and blank lines are neither.
pub(crate) fn count_series(body: &str) -> usize {
    body.lines()
        .filter(|line| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#')
        })
        .count()
}

/// The summed value of every sample of `metric` in an exposition body.
///
/// Summed rather than taken singly because the families this reads are labelled
/// -- `krabka_broker_log_compactions_total` carries a topic and a partition --
/// and the question asked of them is how many cycles the broker ran in total,
/// not how many one partition saw. A name that appears nowhere sums to zero,
/// which is the right answer for a counter that has not been touched.
pub(crate) fn sum_metric(body: &str, metric: &str) -> f64 {
    let mut total = 0.0;
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix(metric) else {
            continue;
        };
        // `foo_total 1` and `foo_total{topic="t"} 1` are members of the family;
        // `foo_total_seconds 1` is a different family that shares a prefix.
        if !(rest.starts_with(' ') || rest.starts_with('{')) {
            continue;
        }
        let Some(value) = rest.split_whitespace().last() else {
            continue;
        };
        if let Ok(parsed) = value.parse::<f64>() {
            total += parsed;
        }
    }
    total
}

/// A segment file's base offset, from its name.
///
/// Kafka's convention, which krabka keeps: twenty zero-padded digits and a
/// `.log` suffix. Index and time-index files share the stem and are not
/// segments.
pub(crate) fn segment_base_offset(file_name: &str) -> Option<u64> {
    file_name.strip_suffix(".log")?.parse().ok()
}

/// Every segment in a formatted log directory, keyed by partition directory.
///
/// Read off the host side of the bind mount, so it sees the same bytes the
/// container is writing.
pub(crate) fn segments(log_dir: &Path) -> BTreeMap<String, BTreeSet<u64>> {
    let mut found = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return found;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(partition) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Ok(files) = std::fs::read_dir(&path) else {
            continue;
        };
        let offsets: BTreeSet<u64> = files
            .filter_map(Result::ok)
            .filter_map(|file| file.file_name().to_str().and_then(segment_base_offset))
            .collect();
        if !offsets.is_empty() {
            found.insert(partition.to_owned(), offsets);
        }
    }
    found
}

/// What the regular files under one directory hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DirectoryUsage {
    pub(crate) bytes: u64,
    pub(crate) files: u64,
}

impl std::ops::Add for DirectoryUsage {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            bytes: self.bytes + other.bytes,
            files: self.files + other.files,
        }
    }
}

impl std::iter::Sum for DirectoryUsage {
    fn sum<I: Iterator<Item = Self>>(usages: I) -> Self {
        usages.fold(Self::default(), std::ops::Add::add)
    }
}

/// Bytes and files held by every regular file under `root`.
///
/// Walked rather than `du`-ed: the image carries no shell and the host may not
/// have one either under a Bazel test sandbox.
pub(crate) fn directory_usage(root: &Path) -> DirectoryUsage {
    let Ok(entries) = std::fs::read_dir(root) else {
        return DirectoryUsage::default();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_usage(&entry.path()),
            Ok(kind) if kind.is_file() => DirectoryUsage {
                bytes: entry.metadata().map_or(0, |m| m.len()),
                files: 1,
            },
            _ => DirectoryUsage::default(),
        })
        .sum()
}

/// [`directory_usage`] of each top-level entry of `log_dir`, by name: one row
/// per partition directory, the metadata log, the audit spool, and each loose
/// file such as `meta.properties`.
///
/// This is what names the directory that grew when the log-directory series
/// fails. A total alone cannot say whether retention, the cleaner, the metadata
/// log or the offsets topic is the one not keeping up.
pub(crate) fn top_level_usage(log_dir: &Path) -> BTreeMap<String, DirectoryUsage> {
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return BTreeMap::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let kind = entry.file_type().ok()?;
            let usage = if kind.is_dir() {
                directory_usage(&entry.path())
            } else if kind.is_file() {
                DirectoryUsage {
                    bytes: entry.metadata().map_or(0, |m| m.len()),
                    files: 1,
                }
            } else {
                return None;
            };
            Some((name, usage))
        })
        .collect()
}

/// Whether `entry` is the directory of a partition of `topic`: `<topic>-<n>`.
pub(crate) fn is_partition_of(entry: &str, topic: &str) -> bool {
    entry
        .strip_prefix(topic)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|index| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
}

/// The usage of every entry in `usage` except the partitions of `excluded`.
pub(crate) fn usage_without(
    usage: &BTreeMap<String, DirectoryUsage>,
    excluded: &str,
) -> DirectoryUsage {
    usage
        .iter()
        .filter(|(entry, _)| !is_partition_of(entry, excluded))
        .map(|(_, usage)| *usage)
        .sum()
}

/// One top-level entry's change in bytes between two samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Growth {
    pub(crate) entry: String,
    pub(crate) bytes: i128,
    pub(crate) files: i128,
}

/// What each top-level entry gained over the second half of `samples`, from
/// the first sample at or past the run's midpoint to the last, largest byte
/// gain first, at most `top` of them. An entry present at one end only counts
/// as zero at the other. Entries that did not change are left out.
///
/// The midpoint is the one the drift judgement fits its trend from, so this names
/// the entries behind a log-directory verdict.
pub(crate) fn second_half_growth(
    samples: &[(std::time::Duration, BTreeMap<String, DirectoryUsage>)],
    top: usize,
) -> Vec<Growth> {
    let (Some((first, _)), Some((last, to))) = (samples.first(), samples.last()) else {
        return Vec::new();
    };
    let midpoint = *first + last.saturating_sub(*first) / 2;
    let Some((_, from)) = samples.iter().find(|(elapsed, _)| *elapsed >= midpoint) else {
        return Vec::new();
    };
    let at = |usage: &BTreeMap<String, DirectoryUsage>, entry: &str| {
        usage.get(entry).copied().unwrap_or_default()
    };
    let entries: BTreeSet<&String> = from.keys().chain(to.keys()).collect();
    let mut growth: Vec<Growth> = entries
        .into_iter()
        .map(|entry| {
            let (before, after) = (at(from, entry), at(to, entry));
            Growth {
                entry: entry.clone(),
                bytes: i128::from(after.bytes) - i128::from(before.bytes),
                files: i128::from(after.files) - i128::from(before.files),
            }
        })
        .filter(|growth| growth.bytes != 0 || growth.files != 0)
        .collect();
    growth.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.entry.cmp(&b.entry)));
    growth.truncate(top);
    growth
}

/// Open descriptors held by `pid`.
///
/// `/proc/<pid>/fd` is mode 500 and owned by the process's user, which is why
/// the fixture runs each container as the host user that owns its data
/// directory. `None` when the process has gone.
pub(crate) fn open_descriptors(pid: u32) -> Option<usize> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    Some(entries.filter_map(Result::ok).count())
}

/// Resident set of `pid`, in bytes. `None` when the process has gone.
pub(crate) fn resident_bytes(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    parse_vm_rss_bytes(&status)
}

mod tests {
    use {super::*, assert2::check};

    /// Equal to within a hair.
    ///
    /// Every value compared in this module is a small integer an `f64`
    /// represents exactly, so the tolerance hides no real difference -- it is
    /// how the comparison is written without a Clippy suppression.
    fn near(a: f64, b: f64) -> bool { (a - b).abs() < 1.0e-9 }

    /// The kernel writes kibibytes; reading them as bytes would understate
    /// every resident-set sample by a factor of 1024 and make the ceiling
    /// meaningless.
    #[test]
    fn vm_rss_is_read_in_kibibytes() {
        let status =
            "Name:\tkrabka-broker\nVmPeak:\t 900000 kB\nVmRSS:\t  262144 kB\nThreads:\t8\n";
        check!(parse_vm_rss_bytes(status) == Some(262_144 * 1024));
    }

    /// A body with no `VmRSS` -- a process that exited mid-read -- is absent,
    /// not zero. Zero would look like a broker using no memory at all and
    /// would drag a mean down rather than fail.
    #[test]
    fn a_status_without_vm_rss_has_no_resident_set() {
        check!(parse_vm_rss_bytes("Name:\tkrabka-broker\n") == None);
        check!(parse_vm_rss_bytes("VmRSS:\t  262144 pages\n") == None);
        check!(parse_vm_rss_bytes("") == None);
    }

    const EXPOSITION: &str = "\
# HELP krabka_broker_active_connections Open connections.
# TYPE krabka_broker_active_connections gauge
krabka_broker_active_connections 12

# HELP krabka_broker_log_cleaner_runs_total Cleaner sweeps.
# TYPE krabka_broker_log_cleaner_runs_total counter
krabka_broker_log_cleaner_runs_total 41
# TYPE krabka_broker_log_compactions_total counter
krabka_broker_log_compactions_total{topic=\"a\",partition=\"0\"} 7
krabka_broker_log_compactions_total{topic=\"a\",partition=\"1\"} 5
krabka_broker_log_compactions_total_seconds 999
";

    /// Cardinality is sample lines. Counting `# HELP`/`# TYPE` would inflate
    /// every reading by roughly the number of families, which is most of the
    /// body.
    #[test]
    fn series_are_the_sample_lines_alone() {
        check!(count_series(EXPOSITION) == 5);
        check!(count_series("") == 0);
        check!(count_series("# HELP only metadata here\n") == 0);
    }

    /// A labelled family sums across its members, and a family that merely
    /// shares a prefix is a different metric.
    #[test]
    fn a_metric_sums_over_its_label_sets() {
        check!(near(
            sum_metric(EXPOSITION, "krabka_broker_log_compactions_total"),
            12.0
        ));
        check!(near(
            sum_metric(EXPOSITION, "krabka_broker_log_cleaner_runs_total"),
            41.0
        ));
        check!(near(
            sum_metric(EXPOSITION, "krabka_broker_log_compactions_total_seconds"),
            999.0
        ));
    }

    /// A counter nobody has touched is not exposed at all, and reads as zero
    /// rather than as an error -- which is what the cycle-count delta needs on
    /// its first sample.
    #[test]
    fn an_absent_metric_is_zero() {
        check!(near(
            sum_metric(EXPOSITION, "krabka_broker_log_cleaner_failures_total"),
            0.0
        ));
    }

    /// Segment names are the zero-padded base offset; the index files beside
    /// them share the stem and are not segments.
    #[test]
    fn segment_names_yield_their_base_offset() {
        check!(segment_base_offset("00000000000000000000.log") == Some(0));
        check!(segment_base_offset("00000000000000004096.log") == Some(4096));
        check!(segment_base_offset("00000000000000004096.index") == None);
        check!(segment_base_offset("leader-epoch-checkpoint") == None);
    }

    /// The walk sums regular files at every depth and ignores a directory it
    /// cannot read, and the top-level rows are one per entry.
    #[test]
    fn directory_usage_sums_the_tree_per_top_level_entry() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join("a"), vec![0u8; 100]).expect("write a");
        std::fs::create_dir_all(dir.path().join("nested/deeper")).expect("mkdir");
        std::fs::write(dir.path().join("nested/b"), vec![0u8; 250]).expect("write b");
        std::fs::write(dir.path().join("nested/deeper/c"), vec![0u8; 50]).expect("write c");
        let usage = |bytes, files| DirectoryUsage { bytes, files };
        check!(directory_usage(dir.path()) == usage(400, 3));
        check!(directory_usage(&dir.path().join("missing")) == DirectoryUsage::default());
        check!(
            top_level_usage(dir.path())
                == BTreeMap::from([
                    ("a".to_owned(), usage(100, 1)),
                    ("nested".to_owned(), usage(300, 2)),
                ])
        );
    }

    /// Only `<topic>-<digits>` is a partition of the topic: not a topic that
    /// shares its prefix, and not the topic name alone.
    #[test]
    fn a_partition_directory_is_the_topic_a_dash_and_an_index() {
        for (entry, topic, want) in [
            ("soak-verify-0", "soak-verify", true),
            ("soak-verify-12", "soak-verify", true),
            ("soak-verify", "soak-verify", false),
            ("soak-verify-", "soak-verify", false),
            ("soak-verify-x", "soak-verify", false),
            ("soak-verify-extra-0", "soak-verify", false),
            ("soak-retention-0", "soak-verify", false),
            ("__cluster_metadata-0", "soak-verify", false),
        ] {
            check!(is_partition_of(entry, topic) == want, "{entry} of {topic}");
        }
    }

    /// The excluded topic's partitions drop out of the sum, and everything
    /// else stays in it.
    #[test]
    fn usage_without_a_topic_leaves_its_partitions_out() {
        let usage = |bytes, files| DirectoryUsage { bytes, files };
        let breakdown = BTreeMap::from([
            ("soak-verify-0".to_owned(), usage(1_000, 3)),
            ("soak-retention-0".to_owned(), usage(200, 4)),
            ("__cluster_metadata-0".to_owned(), usage(30, 5)),
            ("meta.properties".to_owned(), usage(4, 1)),
        ]);
        check!(usage_without(&breakdown, "soak-verify") == usage(234, 10));
        check!(usage_without(&breakdown, "absent") == usage(1_234, 13));
    }

    /// The growth is measured from the first sample at or past the midpoint
    /// to the last, largest byte gain first, and an entry that appears or
    /// disappears counts as zero at the other end.
    #[test]
    fn second_half_growth_names_what_grew_most() {
        use std::time::Duration;
        let usage = |bytes, files| DirectoryUsage { bytes, files };
        let at = |secs, rows: &[(&str, DirectoryUsage)]| {
            (
                Duration::from_secs(secs),
                rows.iter()
                    .map(|(name, usage)| ((*name).to_owned(), *usage))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        let samples = vec![
            at(0, &[("meta", usage(0, 1)), ("offsets", usage(0, 3))]),
            // Before the midpoint of 0..100: not the baseline.
            at(40, &[("meta", usage(900, 1)), ("offsets", usage(900, 3))]),
            at(50, &[
                ("meta", usage(1_000, 7)),
                ("offsets", usage(500, 3)),
                ("gone", usage(80, 1)),
                ("flat", usage(10, 1)),
            ]),
            at(100, &[
                ("meta", usage(1_200, 9)),
                ("offsets", usage(2_500, 3)),
                ("new", usage(300, 2)),
                ("flat", usage(10, 1)),
            ]),
        ];
        let growth = |entry: &str, bytes, files| Growth {
            entry: entry.to_owned(),
            bytes,
            files,
        };
        check!(
            second_half_growth(&samples, 10)
                == vec![
                    growth("offsets", 2_000, 0),
                    growth("new", 300, 2),
                    growth("meta", 200, 2),
                    growth("gone", -80, -1),
                ]
        );
        check!(second_half_growth(&samples, 1) == vec![growth("offsets", 2_000, 0)]);
        check!(second_half_growth(&[], 3).is_empty());
    }

    /// Segments are grouped by partition directory, and a directory holding no
    /// segment does not appear.
    #[test]
    fn segments_are_grouped_by_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (partition, names) in [
            ("t-0", vec![
                "00000000000000000000.log",
                "00000000000000000512.log",
            ]),
            ("t-1", vec!["00000000000000000000.log"]),
            ("__meta", vec!["leader-epoch-checkpoint"]),
        ] {
            let path = dir.path().join(partition);
            std::fs::create_dir(&path).expect("mkdir");
            for name in names {
                std::fs::write(path.join(name), b"x").expect("write");
            }
        }
        let found = segments(dir.path());
        check!(found.keys().collect::<Vec<_>>() == vec!["t-0", "t-1"]);
        check!(found["t-0"] == BTreeSet::from([0, 512]));
        check!(found["t-1"] == BTreeSet::from([0]));
    }

    /// A sample beyond `u32` saturates rather than wrapping, which lands it
    /// above every ceiling in the suite instead of below one.
    #[test]
    fn an_implausible_sample_saturates_above_every_ceiling() {
        check!(near(sampled(1_024), 1_024.0));
        check!(near(sampled(u64::MAX), f64::from(u32::MAX)));
        check!(near(sampled_count(7), 7.0));
    }

    /// This process's own descriptors and resident set are readable, which is
    /// the mechanism the container samples use.
    #[test]
    fn a_live_process_reports_both_readings() {
        let me = std::process::id();
        check!(open_descriptors(me).is_some_and(|n| n > 0));
        check!(resident_bytes(me).is_some_and(|n| n > 0));
    }
}
