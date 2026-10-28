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
    fs,
    iter::Sum,
    ops::Add,
    path::Path,
    time,
};

/// A sampled count as a float, for the trend analysis.
///
/// Every reading this suite takes is bounded by a ceiling well inside `u32` --
/// the largest is a 1.5 GiB resident set -- so the conversion is exact. A
/// reading that does not fit saturates, and `u32::MAX` is above every ceiling
/// here, so an implausible sample fails its series rather than wrapping into a
/// passing one.
pub fn sampled(value: u64) -> f64 { f64::from(u32::try_from(value).unwrap_or(u32::MAX)) }

/// The same, for the `usize` a count comes back as.
pub fn sampled_count(value: usize) -> f64 { sampled(u64::try_from(value).unwrap_or(u64::MAX)) }

/// Resident set size in bytes, from a `/proc/<pid>/status` body.
///
/// `VmRSS` is reported in kibibytes, always: the kernel writes the unit on the
/// line and it has been `kB` for the whole life of the file. Returns `None`
/// when the process has exited between `docker inspect` and this read, which
/// leaves an unreadable or truncated file rather than an error.
pub fn parse_vm_rss_bytes(status: &str) -> Option<u64> {
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
pub fn count_series(body: &str) -> usize {
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
pub fn sum_metric(body: &str, metric: &str) -> f64 {
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
pub fn segment_base_offset(file_name: &str) -> Option<u64> {
    file_name.strip_suffix(".log")?.parse().ok()
}

/// Every segment in a formatted log directory, keyed by partition directory.
///
/// Read off the host side of the bind mount, so it sees the same bytes the
/// container is writing.
pub fn segments(log_dir: &Path) -> BTreeMap<String, BTreeSet<u64>> {
    let mut found = BTreeMap::new();
    let Ok(entries) = fs::read_dir(log_dir) else {
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
        let Ok(files) = fs::read_dir(&path) else {
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
pub struct DirectoryUsage {
    pub(crate) bytes: u64,
    pub(crate) files: u64,
}

impl Add for DirectoryUsage {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            bytes: self.bytes + other.bytes,
            files: self.files + other.files,
        }
    }
}

impl Sum for DirectoryUsage {
    fn sum<I: Iterator<Item = Self>>(usages: I) -> Self { usages.fold(Self::default(), Add::add) }
}

/// Bytes and files held by every regular file under `root`.
///
/// Walked rather than `du`-ed: the image carries no shell and the host may not
/// have one either under a Bazel test sandbox.
pub fn directory_usage(root: &Path) -> DirectoryUsage {
    let Ok(entries) = fs::read_dir(root) else {
        return DirectoryUsage::default();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_usage(&entry.path()),
            Ok(_) => DirectoryUsage {
                bytes: entry.metadata().map_or(0, |m| m.len()),
                files: 1,
            },
            Err(_) => DirectoryUsage::default(),
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
pub fn top_level_usage(log_dir: &Path) -> BTreeMap<String, DirectoryUsage> {
    let Ok(entries) = fs::read_dir(log_dir) else {
        return BTreeMap::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let kind = entry.file_type().ok()?;
            let usage = if kind.is_dir() {
                directory_usage(&entry.path())
            } else {
                DirectoryUsage {
                    bytes: entry.metadata().map_or(0, |m| m.len()),
                    files: 1,
                }
            };
            Some((name, usage))
        })
        .collect()
}

/// Whether `entry` is the directory of a partition of `topic`: `<topic>-<n>`.
pub fn is_partition_of(entry: &str, topic: &str) -> bool {
    entry
        .strip_prefix(topic)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|index| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
}

/// The usage of every entry in `usage` except the partitions of `excluded`.
pub fn usage_without(usage: &BTreeMap<String, DirectoryUsage>, excluded: &str) -> DirectoryUsage {
    usage
        .iter()
        .filter(|(entry, _)| !is_partition_of(entry, excluded))
        .map(|(_, usage)| *usage)
        .sum()
}

/// One top-level entry's change in bytes between two samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Growth {
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
pub fn second_half_growth(
    samples: &[(time::Duration, BTreeMap<String, DirectoryUsage>)],
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
pub fn open_descriptors(pid: u32) -> Option<usize> {
    let entries = fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    Some(entries.filter_map(Result::ok).count())
}

/// Resident set of `pid`, in bytes. `None` when the process has gone.
pub fn resident_bytes(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    parse_vm_rss_bytes(&status)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        std::{fs, process},
        tempfile::TempDir,
    };

    fn near(a: f64, b: f64) -> bool { (a - b).abs() < 1.0e-9 }

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

    #[derive(DataTableRow)]
    struct FileRow {
        path: String,
        bytes: usize,
    }
    #[derive(DataTableRow)]
    struct EntryRow {
        entry: String,
    }
    #[derive(DataTableRow)]
    struct MembershipRow {
        entry: String,
        matches: bool,
    }
    #[derive(DataTableRow)]
    struct UsageRow {
        name: String,
        bytes: u64,
        files: u64,
    }
    #[derive(DataTableRow)]
    struct TimedUsageRow {
        seconds: u64,
        entry: String,
        bytes: u64,
        files: u64,
    }
    #[derive(DataTableRow)]
    struct GrowthRow {
        entry: String,
        bytes: i128,
        files: i128,
    }

    #[derive(Default)]
    struct ProbeScenario {
        statuses: Vec<String>,
        rss: Vec<Option<u64>>,
        exposition: String,
        cardinalities: Vec<usize>,
        sums: BTreeMap<String, f64>,
        segment_names: Vec<String>,
        offsets: Vec<Option<u64>>,
        directory: Option<TempDir>,
        total: Option<DirectoryUsage>,
        missing: Option<DirectoryUsage>,
        breakdown: BTreeMap<String, DirectoryUsage>,
        entries: Vec<String>,
        topic: String,
        membership: BTreeMap<String, bool>,
        exclusions: BTreeMap<String, DirectoryUsage>,
        samples: Vec<(time::Duration, BTreeMap<String, DirectoryUsage>)>,
        growth: Vec<Growth>,
        largest: Vec<Growth>,
        empty_growth: Vec<Growth>,
        grouped_segments: BTreeMap<String, BTreeSet<u64>>,
        converted: Vec<f64>,
        unsigned_samples: Vec<u64>,
        count_sample: usize,
        process: Option<u32>,
        descriptors: Option<usize>,
        resident: Option<u64>,
    }

    #[fixture]
    fn probe() -> ProbeScenario {
        // Every resource probe sees its own inputs and captured observations.
        ProbeScenario {
            directory: None,
            ..Default::default()
        }
    }

    #[given("the kernel status reports VmRSS 262144 kB")]
    fn kernel_rss_status(probe: &mut ProbeScenario) {
        probe.statuses = vec![
            "Name:\tkrabka-broker\nVmPeak:\t 900000 kB\nVmRSS:\t  262144 kB\nThreads:\t8\n".into(),
        ];
    }

    #[given("kernel statuses omit VmRSS use unsupported pages or are empty")]
    fn unreadable_statuses(probe: &mut ProbeScenario) {
        probe.statuses = vec![
            "Name:\tkrabka-broker\n".into(),
            "VmRSS:\t  262144 pages\n".into(),
            String::new(),
        ];
    }

    #[when("the resource probe parses the resident-set statuses")]
    fn parse_resident_sets(probe: &mut ProbeScenario) {
        probe.rss = probe
            .statuses
            .iter()
            .map(|status| parse_vm_rss_bytes(status))
            .collect();
    }

    #[then("the resident set is 268435456 bytes")]
    fn resident_bytes_scaled(probe: &ProbeScenario) {
        check!(probe.rss == [Some(262_144 * 1024)]);
    }

    #[then("all three statuses report no resident-set reading")]
    fn unavailable_rss(probe: &ProbeScenario) {
        check!(probe.rss == [None, None, None]);
    }

    #[given(
        "a scrape contains five samples including labelled compactions and a prefix-neighbor metric"
    )]
    fn labelled_scrape(probe: &mut ProbeScenario) { probe.exposition = EXPOSITION.into(); }

    #[when("the probe counts samples in the scrape an empty body and metadata alone")]
    fn count_sample_lines(probe: &mut ProbeScenario) {
        probe.cardinalities = vec![
            count_series(&probe.exposition),
            count_series(""),
            count_series("# HELP only metadata here\n"),
        ];
    }

    #[then("their metric cardinalities are five zero and zero")]
    fn cardinalities_exclude_metadata(probe: &ProbeScenario) {
        check!(probe.cardinalities == [5, 0, 0]);
    }

    #[when("the probe sums compactions cleaner runs and compaction seconds")]
    fn sum_labelled_families(probe: &mut ProbeScenario) {
        for metric in [
            "krabka_broker_log_compactions_total",
            "krabka_broker_log_cleaner_runs_total",
            "krabka_broker_log_compactions_total_seconds",
        ] {
            probe
                .sums
                .insert(metric.to_owned(), sum_metric(&probe.exposition, metric));
        }
    }

    #[then("compactions sum to 12 cleaner runs to 41 and the distinct seconds metric to 999")]
    fn distinct_family_sums(probe: &ProbeScenario) {
        check!(near(
            probe.sums["krabka_broker_log_compactions_total"],
            12.0
        ));
        check!(near(
            probe.sums["krabka_broker_log_cleaner_runs_total"],
            41.0
        ));
        check!(near(
            probe.sums["krabka_broker_log_compactions_total_seconds"],
            999.0
        ));
    }

    #[when("the probe reads the absent cleaner-failures metric")]
    fn absent_family_sum(probe: &mut ProbeScenario) {
        probe.sums.insert(
            "absent".into(),
            sum_metric(
                &probe.exposition,
                "krabka_broker_log_cleaner_failures_total",
            ),
        );
    }

    #[then("the absent metric has value zero")]
    fn absent_family_is_zero(probe: &ProbeScenario) {
        check!(near(probe.sums["absent"], 0.0));
    }

    #[given("segment filenames start at zero and 4096 beside an index and epoch checkpoint")]
    fn mixed_segment_names(probe: &mut ProbeScenario) {
        probe.segment_names = [
            "00000000000000000000.log",
            "00000000000000004096.log",
            "00000000000000004096.index",
            "leader-epoch-checkpoint",
        ]
        .map(ToOwned::to_owned)
        .to_vec();
    }

    #[when("the probe decodes segment base offsets")]
    fn decode_segment_offsets(probe: &mut ProbeScenario) {
        probe.offsets = probe
            .segment_names
            .iter()
            .map(|name| segment_base_offset(name))
            .collect();
    }

    #[then("only log files yield base offsets zero and 4096")]
    fn only_segments_have_offsets(probe: &ProbeScenario) {
        check!(probe.offsets == [Some(0), Some(4096), None, None]);
    }

    #[given("the log-directory tree contains regular files:")]
    fn directory_files(probe: &mut ProbeScenario, #[datatable] files: Rows<FileRow>) {
        let directory = TempDir::new().expect("tempdir");
        for file in files {
            let path = directory.path().join(file.path);
            fs::create_dir_all(path.parent().expect("a fixture file has a parent")).expect("mkdir");
            fs::write(path, vec![0u8; file.bytes]).expect("write fixture file");
        }
        probe.directory = Some(directory);
    }

    #[when("the probe reads total missing and top-level directory usage")]
    fn read_directory_usage(probe: &mut ProbeScenario) {
        let path = probe
            .directory
            .as_ref()
            .expect("the log-directory tree was created")
            .path();
        probe.total = Some(directory_usage(path));
        probe.missing = Some(directory_usage(&path.join("missing")));
        probe.breakdown = top_level_usage(path);
    }

    #[then("usage is 400 bytes in three files with a 100-byte file and 300-byte nested subtree")]
    fn nested_usage_totals(probe: &ProbeScenario) {
        check!(
            probe.total
                == Some(DirectoryUsage {
                    bytes: 400,
                    files: 3
                })
        );
        check!(probe.missing == Some(DirectoryUsage::default()));
        check!(
            probe.breakdown
                == BTreeMap::from([
                    ("a".to_owned(), DirectoryUsage {
                        bytes: 100,
                        files: 1
                    }),
                    ("nested".to_owned(), DirectoryUsage {
                        bytes: 300,
                        files: 2
                    })
                ])
        );
    }

    #[given("topic {topic} has these candidate partition directories:")]
    fn partition_candidates(
        probe: &mut ProbeScenario,
        topic: String,
        #[datatable] entries: Rows<EntryRow>,
    ) {
        probe.topic = topic;
        probe.entries = entries.into_iter().map(|row| row.entry).collect();
    }

    #[when("the probe identifies exact topic partition directories")]
    fn classify_partition_directories(probe: &mut ProbeScenario) {
        probe.membership = probe
            .entries
            .iter()
            .map(|entry| (entry.clone(), is_partition_of(entry, &probe.topic)))
            .collect();
    }

    #[then("partition-directory membership is:")]
    fn partition_membership(probe: &ProbeScenario, #[datatable] rows: Rows<MembershipRow>) {
        let rows: Vec<_> = rows.into_iter().collect();
        check!(rows.len() == probe.membership.len());
        for row in rows {
            check!(
                probe.membership[&row.entry] == row.matches,
                "{} of {}",
                row.entry,
                probe.topic
            );
        }
    }

    #[given("top-level log-directory usage is:")]
    fn usage_breakdown(probe: &mut ProbeScenario, #[datatable] rows: Rows<UsageRow>) {
        probe.breakdown = rows
            .into_iter()
            .map(|row| {
                (row.name, DirectoryUsage {
                    bytes: row.bytes,
                    files: row.files,
                })
            })
            .collect();
    }

    #[when("the probe excludes soak-verify partitions and separately excludes an absent topic")]
    fn exclude_topic_usage(probe: &mut ProbeScenario) {
        for topic in ["soak-verify", "absent"] {
            probe
                .exclusions
                .insert(topic.into(), usage_without(&probe.breakdown, topic));
        }
    }

    #[then(
        "excluded usage is 234 bytes in ten files and absent-topic usage is 1234 bytes in thirteen"
    )]
    fn topic_exclusion_totals(probe: &ProbeScenario) {
        check!(
            probe.exclusions["soak-verify"]
                == DirectoryUsage {
                    bytes: 234,
                    files: 10
                }
        );
        check!(
            probe.exclusions["absent"]
                == DirectoryUsage {
                    bytes: 1_234,
                    files: 13
                }
        );
    }

    #[given("directory usage was sampled at these elapsed seconds:")]
    fn timed_directory_usage(probe: &mut ProbeScenario, #[datatable] rows: Rows<TimedUsageRow>) {
        let mut samples: BTreeMap<u64, BTreeMap<String, DirectoryUsage>> = BTreeMap::new();
        for row in rows {
            samples
                .entry(row.seconds)
                .or_default()
                .insert(row.entry, DirectoryUsage {
                    bytes: row.bytes,
                    files: row.files,
                });
        }
        probe.samples = samples
            .into_iter()
            .map(|(seconds, usage)| (time::Duration::from_secs(seconds), usage))
            .collect();
    }

    #[when(
        "the probe ranks second-half growth with limits ten and one and checks an empty history"
    )]
    fn rank_directory_growth(probe: &mut ProbeScenario) {
        probe.growth = second_half_growth(&probe.samples, 10);
        probe.largest = second_half_growth(&probe.samples, 1);
        probe.empty_growth = second_half_growth(&[], 3);
    }

    #[then("the ranked directory growth is:")]
    fn ranked_directory_growth(probe: &ProbeScenario, #[datatable] rows: Rows<GrowthRow>) {
        let expected: Vec<_> = rows
            .into_iter()
            .map(|row| Growth {
                entry: row.entry,
                bytes: row.bytes,
                files: row.files,
            })
            .collect();
        check!(probe.growth == expected);
        check!(
            probe.largest
                == vec![Growth {
                    entry: "offsets".to_owned(),
                    bytes: 2_000,
                    files: 0
                }]
        );
        check!(probe.empty_growth.is_empty());
    }

    #[when("the probe groups segment files by partition")]
    fn group_partition_segments(probe: &mut ProbeScenario) {
        probe.grouped_segments = segments(
            probe
                .directory
                .as_ref()
                .expect("the segment tree was created")
                .path(),
        );
    }

    #[then("only t-0 and t-1 contain segments with bases zero-512 and zero respectively")]
    fn grouped_log_segments(probe: &ProbeScenario) {
        check!(probe.grouped_segments.keys().collect::<Vec<_>>() == vec!["t-0", "t-1"]);
        check!(probe.grouped_segments["t-0"] == BTreeSet::from([0, 512]));
        check!(probe.grouped_segments["t-1"] == BTreeSet::from([0]));
    }

    #[given("sample readings include 1024 the maximum u64 and a count of seven")]
    fn sample_conversion_inputs(probe: &mut ProbeScenario) {
        probe.unsigned_samples = vec![1_024, u64::MAX];
        probe.count_sample = 7;
    }

    #[when("the probe converts the readings to trend-analysis numbers")]
    fn convert_sample_readings(probe: &mut ProbeScenario) {
        probe.converted = probe
            .unsigned_samples
            .iter()
            .copied()
            .map(sampled)
            .collect();
        probe.converted.push(sampled_count(probe.count_sample));
    }

    #[then("the readings are 1024 the maximum u32 and seven without wrapping")]
    fn implausible_sample_saturates(probe: &ProbeScenario) {
        check!(near(probe.converted[0], 1_024.0));
        check!(near(probe.converted[1], f64::from(u32::MAX)));
        check!(near(probe.converted[2], 7.0));
    }

    #[given("the resource probe targets this live process")]
    fn live_process_target(probe: &mut ProbeScenario) { probe.process = Some(process::id()); }

    #[when("the probe reads live descriptors and resident memory")]
    fn read_process_resources(probe: &mut ProbeScenario) {
        let process = probe.process.expect("a live process was selected");
        probe.descriptors = open_descriptors(process);
        probe.resident = resident_bytes(process);
    }

    #[then("the process reports positive descriptor and resident-memory readings")]
    fn live_process_readings(probe: &ProbeScenario) {
        check!(probe.descriptors.is_some_and(|descriptors| descriptors > 0));
        check!(probe.resident.is_some_and(|resident| resident > 0));
    }

    scenarios!("tests/features/migrated/tests/soak/probe.feature", fixtures = [probe: ProbeScenario]);
}
