//! Resource and controller-progress telemetry, sampled for the whole run.
//!
//! Every `interval`, each node gets one row in `telemetry.jsonl`: its
//! container's CPU and memory as `docker stats` reports them, the bytes its
//! data directory holds and the free bytes beside it, and the controller
//! progress its `/metrics` body reports. A row is appended and flushed as it is
//! taken, so a run that dies at hour 70 keeps every row up to then.
//!
//! A node that does not answer its scrape, because it is paused or restarting,
//! still gets a row, with `scraped` false and no `progress`. A gap in the trend
//! is evidence too.

use {
    crate::{
        host::{docker_try, free_bytes, now_ms},
        probe::directory_usage,
        topology::{Node, Nodes},
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fmt::Display,
        fs::{File, OpenOptions},
        io::Write as _,
        path::Path,
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::oneshot,
        task::JoinHandle,
        time::{MissedTickBehavior, interval as ticker},
    },
};

/// The file the rows go to, in the evidence directory.
const TELEMETRY_FILE: &str = "telemetry.jsonl";

const ACTIVE_CONTROLLER: &str = "krabka_broker_active_controller";
const RAFT_CURRENT_STATE: &str = "krabka_broker_raft_current_state";
const RAFT_CURRENT_EPOCH: &str = "krabka_broker_raft_current_epoch";
const RAFT_HIGH_WATERMARK: &str = "krabka_broker_raft_high_watermark";
const LAST_APPLIED_OFFSET: &str = "krabka_broker_metadata_last_applied_offset";
const LAST_COMMITTED_OFFSET: &str = "krabka_broker_metadata_last_committed_offset";
const LAST_APPLIED_TIMESTAMP_MS: &str = "krabka_broker_metadata_last_applied_timestamp_ms";
const LAST_APPLIED_LAG_MS: &str = "krabka_broker_metadata_last_applied_lag_ms";
const METADATA_LAG_RECORDS: &str = "krabka_broker_metadata_lag_records";
const CONTROLLER_LEADER_CHANGES: &str = "krabka_broker_controller_leader_changes_total";
const UNDER_REPLICATED: &str = "krabka_broker_under_replicated_partitions";
const UNDER_MIN_ISR: &str = "krabka_broker_under_min_isr_partition_count";
const OFFLINE: &str = "krabka_broker_offline_partitions_count";

/// What reads container resources: given container names, the output of
/// `docker stats --no-stream --format '{{json .}}'` for them.
type StatsSource = fn(&[String]) -> Result<String, String>;

/// One `docker stats` call for every container.
fn docker_stats(names: &[String]) -> Result<String, String> {
    let mut args = vec!["stats", "--no-stream", "--format", "{{json .}}"];
    args.extend(names.iter().map(String::as_str));
    docker_try(&args)
}

/// One container's line of `docker stats --format '{{json .}}'`, reduced to
/// the fields the telemetry keeps.
#[derive(Debug, Deserialize)]
struct StatsLine {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "CPUPerc", default)]
    cpu: String,
    #[serde(rename = "MemUsage", default)]
    memory: String,
}

/// A container's resources at one sample.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Usage {
    pub(crate) cpu_percent: Option<f64>,
    pub(crate) memory_bytes: Option<u64>,
}

/// The usage of each container in a `docker stats` output, by container name.
/// A line that does not parse is left out.
pub(crate) fn parse_stats(output: &str) -> BTreeMap<String, Usage> {
    output
        .lines()
        .filter_map(|line| serde_json::from_str::<StatsLine>(line.trim()).ok())
        .map(|line| {
            let usage = Usage {
                cpu_percent: parse_percent(&line.cpu),
                memory_bytes: parse_size(
                    line.memory
                        .split_once('/')
                        .map_or(line.memory.as_str(), |(used, _)| used),
                ),
            };
            (line.name, usage)
        })
        .collect()
}

/// A percentage such as `12.34%`; `None` for `--` or anything else.
pub(crate) fn parse_percent(text: &str) -> Option<f64> {
    text.trim()
        .strip_suffix('%')?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|percent| percent.is_finite())
}

/// A size such as `512.3MiB`, `1.2kB` or `0B` in bytes, with decimal (`kB`,
/// `MB`) and binary (`KiB`, `MiB`) units as Docker prints them. The fraction
/// is truncated to whole bytes.
pub(crate) fn parse_size(text: &str) -> Option<u64> {
    let text = text.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let multiplier: u128 = match unit.trim() {
        "" | "B" => 1,
        "kB" | "KB" => 1_000,
        "KiB" => 1 << 10,
        "MB" => 1_000_000,
        "MiB" => 1 << 20,
        "GB" => 1_000_000_000,
        "GiB" => 1 << 30,
        "TB" => 1_000_000_000_000,
        "TiB" => 1 << 40,
        _ => return None,
    };
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    // Eighteen digits keep `10^digits` inside a u128 product.
    let fraction = &fraction[..fraction.len().min(18)];
    let whole: u128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let numerator: u128 = if fraction.is_empty() {
        0
    } else {
        fraction.parse().ok()?
    };
    let scale = 10u128.pow(u32::try_from(fraction.len()).ok()?);
    u64::try_from(whole * multiplier + numerator * multiplier / scale).ok()
}

/// The value text of the first sample of `metric` in an exposition body.
fn sample_text<'a>(body: &'a str, metric: &str) -> Option<&'a str> {
    body.lines().map(str::trim).find_map(|line| {
        if line.starts_with('#') {
            return None;
        }
        let rest = line.strip_prefix(metric)?;
        // `foo 1` and `foo{a="b"} 1` are the family; `foo_bar 1` is not.
        (rest.starts_with(' ') || rest.starts_with('{'))
            .then(|| rest.split_whitespace().last())
            .flatten()
    })
}

/// The first sample of `metric` as a whole number, rounded; `None` when the
/// body has no sample of it.
pub(crate) fn gauge(body: &str, metric: &str) -> Option<i64> {
    let text = sample_text(body, metric)?;
    text.parse::<i64>().ok().or_else(|| {
        let value = text.parse::<f64>().ok().filter(|value| value.is_finite())?;
        format!("{value:.0}").parse().ok()
    })
}

/// The `state` label of the `krabka_broker_raft_current_state` series that
/// reads 1, when one does.
pub(crate) fn raft_state(body: &str) -> Option<String> {
    body.lines().map(str::trim).find_map(|line| {
        let labels = line.strip_prefix(RAFT_CURRENT_STATE)?.strip_prefix('{')?;
        let (labels, value) = labels.split_once('}')?;
        let value: f64 = value.trim().parse().ok()?;
        let state = labels
            .split(',')
            .find_map(|label| label.trim().strip_prefix("state=\""))?
            .strip_suffix('"')?;
        (value >= 1.0).then(|| state.to_owned())
    })
}

/// What one node's `/metrics` body says of the controller's progress. A field
/// is `None` when the body has no sample of its series, as on a broker-only
/// node for the `raft_*` series.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Progress {
    pub(crate) active_controller: Option<bool>,
    pub(crate) raft_state: Option<String>,
    pub(crate) raft_current_epoch: Option<i64>,
    pub(crate) raft_high_watermark: Option<i64>,
    pub(crate) metadata_last_applied_offset: Option<i64>,
    pub(crate) metadata_last_committed_offset: Option<i64>,
    pub(crate) metadata_last_applied_timestamp_ms: Option<i64>,
    pub(crate) metadata_last_applied_lag_ms: Option<i64>,
    pub(crate) metadata_lag_records: Option<i64>,
    pub(crate) controller_leader_changes_total: Option<i64>,
    pub(crate) under_replicated_partitions: Option<i64>,
    pub(crate) under_min_isr_partitions: Option<i64>,
    pub(crate) offline_partitions: Option<i64>,
}

/// The controller progress in a `/metrics` body.
pub(crate) fn progress(body: &str) -> Progress {
    Progress {
        active_controller: gauge(body, ACTIVE_CONTROLLER).map(|value| value >= 1),
        raft_state: raft_state(body),
        raft_current_epoch: gauge(body, RAFT_CURRENT_EPOCH),
        raft_high_watermark: gauge(body, RAFT_HIGH_WATERMARK),
        metadata_last_applied_offset: gauge(body, LAST_APPLIED_OFFSET),
        metadata_last_committed_offset: gauge(body, LAST_COMMITTED_OFFSET),
        metadata_last_applied_timestamp_ms: gauge(body, LAST_APPLIED_TIMESTAMP_MS),
        metadata_last_applied_lag_ms: gauge(body, LAST_APPLIED_LAG_MS),
        metadata_lag_records: gauge(body, METADATA_LAG_RECORDS),
        controller_leader_changes_total: gauge(body, CONTROLLER_LEADER_CHANGES),
        under_replicated_partitions: gauge(body, UNDER_REPLICATED),
        under_min_isr_partitions: gauge(body, UNDER_MIN_ISR),
        offline_partitions: gauge(body, OFFLINE),
    }
}

/// One node at one sample: a line of `telemetry.jsonl`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TelemetryRow {
    /// The sample this row belongs to, counted from 0. Every node's row of one
    /// sample carries the same tick.
    pub(crate) tick: u64,
    pub(crate) unix_ms: i64,
    pub(crate) node: i32,
    pub(crate) container: String,
    pub(crate) cpu_percent: Option<f64>,
    pub(crate) memory_bytes: Option<u64>,
    pub(crate) data_dir_bytes: u64,
    pub(crate) data_dir_free_bytes: Option<u64>,
    /// Whether the node answered its `/metrics` scrape.
    pub(crate) scraped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) progress: Option<Progress>,
}

/// What one node's rows add up to.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(crate) struct NodeSummary {
    pub(crate) rows: u64,
    pub(crate) unanswered: u64,
    pub(crate) peak_cpu_percent: Option<f64>,
    pub(crate) peak_memory_bytes: Option<u64>,
    pub(crate) peak_data_dir_bytes: u64,
    pub(crate) min_free_bytes: Option<u64>,
    pub(crate) max_metadata_lag_records: Option<i64>,
    /// Counted only where the last applied timestamp is above zero: before the
    /// first record the lag reads the Unix time.
    pub(crate) max_metadata_lag_ms: Option<i64>,
    /// Increments of `krabka_broker_controller_leader_changes_total` seen
    /// after the node's first answered scrape. A counter that falls has been
    /// reset by a restart, and its new value counts whole.
    pub(crate) controller_leader_changes: i64,
}

/// What the whole run's rows add up to.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(crate) struct TelemetrySummary {
    pub(crate) ticks: u64,
    pub(crate) rows: u64,
    pub(crate) unanswered: u64,
    /// How often the node reporting itself the active controller changed from
    /// one such report to the next.
    pub(crate) active_controller_changes: u64,
    pub(crate) nodes: BTreeMap<i32, NodeSummary>,
}

impl TelemetrySummary {
    /// The end-of-run printout.
    pub(crate) fn lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "telemetry: {} rows over {} samples, {} unanswered scrapes, {} active-controller \
             changes",
            self.rows, self.ticks, self.unanswered, self.active_controller_changes
        )];
        lines.extend(self.nodes.iter().map(|(id, node)| {
            format!(
                "  node {id}: peak cpu {}%, peak memory {} B, peak data dir {} B, min free {} B, \
                 max metadata lag {} records / {} ms, {} controller leader changes, {}/{} \
                 unanswered",
                shown(node.peak_cpu_percent),
                shown(node.peak_memory_bytes),
                node.peak_data_dir_bytes,
                shown(node.min_free_bytes),
                shown(node.max_metadata_lag_records),
                shown(node.max_metadata_lag_ms),
                node.controller_leader_changes,
                node.unanswered,
                node.rows,
            )
        }));
        lines
    }
}

/// `value`, or `-` when there is none.
fn shown(value: Option<impl Display>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

/// The larger of two optional values, either one when the other is absent.
fn peak<T: PartialOrd + Copy>(held: Option<T>, seen: Option<T>) -> Option<T> {
    match (held, seen) {
        (Some(held), Some(seen)) => Some(if seen > held { seen } else { held }),
        (held, seen) => held.or(seen),
    }
}

/// The summary, and what folding the next row needs to remember.
#[derive(Debug, Default)]
struct Fold {
    summary: TelemetrySummary,
    last_tick: Option<u64>,
    last_controller: Option<i32>,
    last_leader_changes: BTreeMap<i32, i64>,
}

impl Fold {
    fn record(&mut self, row: &TelemetryRow) {
        let summary = &mut self.summary;
        if self.last_tick != Some(row.tick) {
            summary.ticks += 1;
            self.last_tick = Some(row.tick);
        }
        summary.rows += 1;
        let node = summary.nodes.entry(row.node).or_default();
        node.rows += 1;
        node.peak_cpu_percent = peak(node.peak_cpu_percent, row.cpu_percent);
        node.peak_memory_bytes = peak(node.peak_memory_bytes, row.memory_bytes);
        node.peak_data_dir_bytes = node.peak_data_dir_bytes.max(row.data_dir_bytes);
        node.min_free_bytes = match (node.min_free_bytes, row.data_dir_free_bytes) {
            (Some(held), Some(seen)) => Some(held.min(seen)),
            (held, seen) => held.or(seen),
        };
        let Some(progress) = &row.progress else {
            node.unanswered += 1;
            summary.unanswered += 1;
            return;
        };
        node.max_metadata_lag_records =
            peak(node.max_metadata_lag_records, progress.metadata_lag_records);
        if progress
            .metadata_last_applied_timestamp_ms
            .is_some_and(|timestamp| timestamp > 0)
        {
            node.max_metadata_lag_ms = peak(
                node.max_metadata_lag_ms,
                progress.metadata_last_applied_lag_ms,
            );
        }
        if let Some(changes) = progress.controller_leader_changes_total
            && let Some(last) = self.last_leader_changes.insert(row.node, changes)
        {
            node.controller_leader_changes += if changes >= last {
                changes - last
            } else {
                changes
            };
        }
        if progress.active_controller == Some(true) {
            if self
                .last_controller
                .is_some_and(|controller| controller != row.node)
            {
                summary.active_controller_changes += 1;
            }
            self.last_controller = Some(row.node);
        }
    }
}

/// The summary of `rows`, folded in order.
#[cfg(test)]
fn summarize(rows: &[TelemetryRow]) -> TelemetrySummary {
    let mut fold = Fold::default();
    for row in rows {
        fold.record(row);
    }
    fold.summary
}

/// `telemetry.jsonl`, appended and flushed one row at a time.
struct TelemetryLog {
    file: Option<File>,
}

impl TelemetryLog {
    fn create(dir: &Path) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(TELEMETRY_FILE));
        if let Err(error) = &file {
            eprintln!("telemetry: open {TELEMETRY_FILE}: {error}; rows are summarized only");
        }
        Self { file: file.ok() }
    }

    fn append(&mut self, row: &TelemetryRow) {
        let (Some(file), Ok(line)) = (self.file.as_mut(), serde_json::to_string(row)) else {
            return;
        };
        // Best effort: a full disk must not stop the run it is measuring.
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

/// One row per node, all at one tick.
async fn sample(nodes: &Nodes, tick: u64, stats: StatsSource) -> Vec<TelemetryRow> {
    let unix_ms = now_ms();
    let members: Vec<Node> = nodes
        .ids()
        .into_iter()
        .filter_map(|id| nodes.node(id).cloned())
        .collect();
    let names: Vec<String> = members.iter().map(|node| node.container.clone()).collect();
    let roots: Vec<_> = members.iter().map(|node| node.root.clone()).collect();
    // `docker stats --no-stream` takes a couple of seconds and the walk reads
    // every segment's metadata, so both run off the runtime's threads.
    let (usage, disks) = tokio::task::spawn_blocking(move || {
        let usage = if names.is_empty() {
            BTreeMap::new()
        } else {
            stats(&names)
                .map(|output| parse_stats(&output))
                .unwrap_or_default()
        };
        let disks: Vec<(u64, Option<u64>)> = roots
            .iter()
            .map(|root| (directory_usage(root).bytes, free_bytes(root)))
            .collect();
        (usage, disks)
    })
    .await
    .unwrap_or_default();
    let mut rows = Vec::with_capacity(members.len());
    for (index, node) in members.into_iter().enumerate() {
        let body = nodes.scrape(node.id).await;
        let resources = usage.get(&node.container).copied().unwrap_or_default();
        let (data_dir_bytes, data_dir_free_bytes) = disks.get(index).copied().unwrap_or_default();
        rows.push(TelemetryRow {
            tick,
            unix_ms,
            node: node.id,
            container: node.container,
            cpu_percent: resources.cpu_percent,
            memory_bytes: resources.memory_bytes,
            data_dir_bytes,
            data_dir_free_bytes,
            scraped: body.is_some(),
            progress: body.as_deref().map(progress),
        });
    }
    rows
}

/// Sample every `period` until `stop` fires or is dropped, then once more.
async fn run(
    nodes: Arc<Nodes>,
    mut log: TelemetryLog,
    period: Duration,
    stats: StatsSource,
    mut stop: oneshot::Receiver<()>,
) -> TelemetrySummary {
    let mut fold = Fold::default();
    let mut ticks = ticker(period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut tick = 0;
    loop {
        let stopping = tokio::select! {
            _ = ticks.tick() => false,
            _ = &mut stop => true,
        };
        for row in sample(&nodes, tick, stats).await {
            log.append(&row);
            fold.record(&row);
        }
        tick += 1;
        if stopping {
            return fold.summary;
        }
    }
}

/// The run's telemetry sampler, running on its own task.
pub(crate) struct Telemetry {
    stop: oneshot::Sender<()>,
    task: JoinHandle<TelemetrySummary>,
}

impl Telemetry {
    /// Start sampling `nodes` every `interval` into `dir/telemetry.jsonl`. The
    /// first sample is taken at once.
    pub(crate) fn start(nodes: Arc<Nodes>, dir: &Path, interval: Duration) -> Self {
        Self::spawn(nodes, dir, interval, docker_stats)
    }

    fn spawn(nodes: Arc<Nodes>, dir: &Path, interval: Duration, stats: StatsSource) -> Self {
        let (stop, stopped) = oneshot::channel();
        let log = TelemetryLog::create(dir);
        Self {
            stop,
            task: tokio::spawn(run(nodes, log, interval, stats, stopped)),
        }
    }

    /// Take one last sample, stop, and sum up every row taken.
    pub(crate) async fn stop(self) -> TelemetrySummary {
        // The task may already have ended, and then there is no one to tell.
        let _ = self.stop.send(());
        self.task.await.unwrap_or_else(|error| {
            eprintln!("telemetry: the sampler failed: {error}");
            TelemetrySummary::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            NodeSummary, Progress, Telemetry, TelemetryRow, TelemetrySummary, Usage, gauge,
            parse_percent, parse_size, parse_stats, progress, raft_state, summarize,
        },
        crate::{
            cluster::Launch,
            topology::{Node, Nodes},
        },
        assert2::{assert, check},
        std::{collections::BTreeMap, sync::Arc, time::Duration},
    };

    #[yare::parameterized(
        mebibytes = { "512.3MiB", Some(537_185_484) },
        gibibytes = { "1.5GiB", Some(1_610_612_736) },
        kibibytes = { "4KiB", Some(4_096) },
        kilobytes = { "12kB", Some(12_000) },
        upper_kilobytes = { "1.2KB", Some(1_200) },
        megabytes = { "3.25MB", Some(3_250_000) },
        bytes = { "0B", Some(0) },
        bare_number = { "17", Some(17) },
        padded = { " 2GiB ", Some(2_147_483_648) },
        no_number = { "MiB", None },
        unknown_unit = { "3 parsecs", None },
        two_points = { "1.2.3MiB", None },
        placeholder = { "--", None },
    )]
    fn a_size_reads_in_bytes(text: &str, bytes: Option<u64>) {
        check!(parse_size(text) == bytes);
    }

    #[yare::parameterized(
        plain = { "12.34%", Some(12.34) },
        over_one_core = { "250.00%", Some(250.0) },
        zero = { "0.00%", Some(0.0) },
        placeholder = { "--", None },
        no_sign = { "12.34", None },
    )]
    fn a_percentage_reads_as_a_number(text: &str, percent: Option<f64>) {
        check!(parse_percent(text) == percent);
    }

    #[test]
    fn docker_stats_reads_by_container_name() {
        let output = concat!(
            r#"{"BlockIO":"0B / 0B","CPUPerc":"12.34%","Container":"k1","ID":"a","MemPerc":"3.2%","MemUsage":"512MiB / 15.6GiB","Name":"k1","NetIO":"1kB / 2kB","PIDs":"40"}"#,
            "\n",
            r#"{"CPUPerc":"--","MemUsage":"-- / --","Name":"k2"}"#,
            "\n",
            "not json\n",
        );
        let expected = BTreeMap::from([
            ("k1".to_owned(), Usage {
                cpu_percent: Some(12.34),
                memory_bytes: Some(512 << 20),
            }),
            ("k2".to_owned(), Usage::default()),
        ]);
        check!(parse_stats(output) == expected);
    }

    const BODY: &str = "\
# HELP krabka_broker_active_controller 1 if this broker is the raft (controller) leader.
# TYPE krabka_broker_active_controller gauge
krabka_broker_active_controller 1
krabka_broker_controller_leader_changes_total 3
krabka_broker_raft_current_state{state=\"candidate\"} 0
krabka_broker_raft_current_state{state=\"leader\"} 1
krabka_broker_raft_current_epoch 7
krabka_broker_raft_high_watermark 1.2e3
krabka_broker_metadata_last_applied_offset 1199
krabka_broker_metadata_last_applied_offset_extra 5
krabka_broker_metadata_last_committed_offset 1200
krabka_broker_metadata_last_applied_timestamp_ms 1700000000000
krabka_broker_metadata_last_applied_lag_ms 480
krabka_broker_metadata_lag_records 1
krabka_broker_under_replicated_partitions 2
krabka_broker_under_min_isr_partition_count 0
krabka_broker_offline_partitions_count 0
";

    #[test]
    fn a_body_reads_as_controller_progress() {
        check!(
            progress(BODY)
                == Progress {
                    active_controller: Some(true),
                    raft_state: Some("leader".to_owned()),
                    raft_current_epoch: Some(7),
                    raft_high_watermark: Some(1200),
                    metadata_last_applied_offset: Some(1199),
                    metadata_last_committed_offset: Some(1200),
                    metadata_last_applied_timestamp_ms: Some(1_700_000_000_000),
                    metadata_last_applied_lag_ms: Some(480),
                    metadata_lag_records: Some(1),
                    controller_leader_changes_total: Some(3),
                    under_replicated_partitions: Some(2),
                    under_min_isr_partitions: Some(0),
                    offline_partitions: Some(0),
                }
        );
    }

    #[test]
    fn a_body_without_the_series_reads_as_unknown() {
        check!(progress("# nothing here\n") == Progress::default());
    }

    #[yare::parameterized(
        integer = { "m 42\n", Some(42) },
        negative = { "m -1\n", Some(-1) },
        float = { "m 2.6\n", Some(3) },
        labelled = { "m{a=\"b\"} 9\n", Some(9) },
        prefix_only = { "m_other 9\n", None },
        comment = { "# m 9\n", None },
        not_a_number = { "m NaN\n", None },
        absent = { "", None },
    )]
    fn a_gauge_is_its_first_sample(body: &str, value: Option<i64>) {
        check!(gauge(body, "m") == value);
    }

    #[yare::parameterized(
        follower = { "krabka_broker_raft_current_state{state=\"leader\"} 0\nkrabka_broker_raft_current_state{state=\"follower\"} 1\n", Some("follower") },
        none_set = { "krabka_broker_raft_current_state{state=\"leader\"} 0\n", None },
        broker_only = { "krabka_broker_active_controller 0\n", None },
    )]
    fn the_raft_state_is_the_label_that_reads_one(body: &str, state: Option<&str>) {
        check!(raft_state(body).as_deref() == state);
    }

    struct At {
        tick: u64,
        node: i32,
        memory: Option<u64>,
        disk: u64,
        lag: i64,
        changes: i64,
        controller: bool,
    }

    fn row(at: &At, answered: bool) -> TelemetryRow {
        TelemetryRow {
            tick: at.tick,
            unix_ms: 0,
            node: at.node,
            container: format!("k{}", at.node),
            cpu_percent: Some(1.0),
            memory_bytes: at.memory,
            data_dir_bytes: at.disk,
            data_dir_free_bytes: Some(1_000 - at.disk),
            scraped: answered,
            progress: answered.then(|| Progress {
                active_controller: Some(at.controller),
                metadata_last_applied_timestamp_ms: Some(1),
                metadata_last_applied_lag_ms: Some(at.lag * 100),
                metadata_lag_records: Some(at.lag),
                controller_leader_changes_total: Some(at.changes),
                ..Progress::default()
            }),
        }
    }

    #[test]
    fn rows_fold_into_peaks_gaps_and_controller_changes() {
        let at = |tick, node, memory, disk, lag, changes, controller| At {
            tick,
            node,
            memory,
            disk,
            lag,
            changes,
            controller,
        };
        let rows = vec![
            row(&at(0, 1, Some(100), 10, 0, 4, true), true),
            row(&at(0, 2, Some(200), 20, 2, 1, false), true),
            // Node 1 is paused: no scrape, and docker reports no memory.
            row(&at(1, 1, None, 15, 0, 0, false), false),
            row(&at(1, 2, Some(150), 30, 5, 2, true), true),
            // Node 1 restarted: its counter fell from 4 to 1, and the 1 counts.
            row(&at(2, 1, Some(50), 12, 1, 1, false), true),
            row(&at(2, 2, Some(180), 25, 0, 2, true), true),
        ];
        let expected = TelemetrySummary {
            ticks: 3,
            rows: 6,
            unanswered: 1,
            active_controller_changes: 1,
            nodes: BTreeMap::from([
                (1, NodeSummary {
                    rows: 3,
                    unanswered: 1,
                    peak_cpu_percent: Some(1.0),
                    peak_memory_bytes: Some(100),
                    peak_data_dir_bytes: 15,
                    min_free_bytes: Some(985),
                    max_metadata_lag_records: Some(1),
                    max_metadata_lag_ms: Some(100),
                    controller_leader_changes: 1,
                }),
                (2, NodeSummary {
                    rows: 3,
                    unanswered: 0,
                    peak_cpu_percent: Some(1.0),
                    peak_memory_bytes: Some(200),
                    peak_data_dir_bytes: 30,
                    min_free_bytes: Some(970),
                    max_metadata_lag_records: Some(5),
                    max_metadata_lag_ms: Some(500),
                    controller_leader_changes: 1,
                }),
            ]),
        };
        let summary = summarize(&rows);
        check!(summary == expected);
        check!(summary.lines().len() == 3);
        check!(summary.lines()[0].contains("6 rows over 3 samples, 1 unanswered"));
    }

    #[test]
    fn lag_before_the_first_record_is_not_counted() {
        let mut first = row(
            &At {
                tick: 0,
                node: 1,
                memory: None,
                disk: 0,
                lag: 9,
                changes: 0,
                controller: false,
            },
            true,
        );
        if let Some(progress) = first.progress.as_mut() {
            progress.metadata_last_applied_timestamp_ms = Some(-1);
        }
        check!(summarize(&[first]).nodes[&1].max_metadata_lag_ms == None);
    }

    #[tokio::test]
    async fn an_unanswering_node_still_gets_a_row_for_every_sample() {
        let evidence = tempfile::tempdir().expect("an evidence directory");
        let root = tempfile::tempdir().expect("a data directory");
        std::fs::write(root.path().join("segment"), [0u8; 10]).expect("write a segment");
        let nodes = Arc::new(Nodes::new(vec![Node {
            id: 1,
            container: "k1".to_owned(),
            bootstrap: "127.0.0.1:9".to_owned(),
            // The discard port: nothing listens, so the scrape is refused.
            metrics_url: "http://127.0.0.1:9/metrics".to_owned(),
            root: root.path().to_path_buf(),
            launch: Launch::default(),
            tls: None,
        }]));
        let telemetry =
            Telemetry::spawn(nodes, evidence.path(), Duration::from_millis(20), |names| {
                Ok(names
                    .iter()
                    .map(|name| {
                        format!(r#"{{"Name":"{name}","CPUPerc":"5.00%","MemUsage":"1KiB / 1GiB"}}"#)
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let summary = telemetry.stop().await;

        let text =
            std::fs::read_to_string(evidence.path().join("telemetry.jsonl")).expect("rows written");
        let rows: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON row"))
            .collect();
        assert!(rows.len() >= 2);
        check!(u64::try_from(rows.len()) == Ok(summary.rows));
        check!(summary.unanswered == summary.rows);
        let first = &rows[0];
        check!(first["node"] == 1);
        check!(first["container"] == "k1");
        check!(first["cpu_percent"] == 5.0);
        check!(first["memory_bytes"] == 1024);
        check!(first["data_dir_bytes"] == 10);
        check!(first["scraped"] == false);
        check!(first.get("progress").is_none());
    }

    #[tokio::test]
    async fn a_cluster_of_no_nodes_starts_and_stops() {
        let evidence = tempfile::tempdir().expect("an evidence directory");
        let telemetry = Telemetry::start(
            Arc::new(Nodes::new(Vec::new())),
            evidence.path(),
            Duration::from_millis(10),
        );
        check!(telemetry.stop().await == TelemetrySummary::default());
        check!(evidence.path().join("telemetry.jsonl").exists());
    }
}
