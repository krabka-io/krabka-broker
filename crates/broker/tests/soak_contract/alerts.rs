//! The alert history of the run, from a Prometheus sidecar that loads
//! `docs/operations/alert-rules.yaml`.
//!
//! The sidecar scrapes every broker's `/metrics` as the job `krabka-broker`,
//! the job the shipped rules name, and evaluates the shipped rules unchanged.
//! It reaches the brokers the way they reach each other: through
//! `host.docker.internal`, which `--add-host=host.docker.internal:host-gateway`
//! resolves to the host, where each broker publishes its metrics port.
//!
//! The history is Prometheus's own `ALERTS` series, read back with
//! `query_range` at the evaluation interval and folded into one list of
//! pending and firing intervals per alert instance. The rules Prometheus
//! loaded are read back too, so a rule that does not evaluate is a finding
//! rather than an alert that silently never fires.

use {
    crate::{
        host::{docker_try, now_ms, write_json},
        support::{manifest_dir, remove_container_with_volumes},
        topology::Nodes,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs::Permissions,
        io::{Read as _, Write as _},
        net::TcpStream,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// The Prometheus image the sidecar runs: v3.5.0, the tag
/// `//bazel/images:prometheus_3_5_0` loads the digest `//MODULE.bazel` pins
/// under. Under Cargo, `docker run` pulls the tag on first use.
const PROMETHEUS_IMAGE: &str = "mirror.gcr.io/prom/prometheus:v3.5.0";

/// The scrape job the shipped rules select on, as in `up{job="krabka-broker"}`.
const JOB: &str = "krabka-broker";

/// The scrape and rule-evaluation interval, in seconds. Short enough that a
/// fault of a few minutes moves a rule through `for` and back.
const INTERVAL_SECS: i64 = 5;

/// How long one scrape may take. Below the interval, so a paused broker reads
/// as `up == 0` within one interval.
const SCRAPE_TIMEOUT_SECS: i64 = 4;

/// How long the TSDB keeps samples: more than a 72-hour run and its settle.
const RETENTION: &str = "8d";

/// Where the configuration directory mounts in the container.
const CONTAINER_CONFIG_DIR: &str = "/etc/prometheus/soak";

/// The name the rules copy has in the configuration directory.
const RULES_FILE: &str = "alert-rules.yaml";

/// The name of the Prometheus configuration, in the configuration directory
/// and in the evidence.
const CONFIG_FILE: &str = "prometheus.yml";

/// The evidence file the history is written to.
const EVIDENCE_FILE: &str = "alerts.json";

/// How long Prometheus may take to answer `/-/ready`.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// The most points one `query_range` series may hold. Prometheus refuses a
/// range of more than 11 000; a 72-hour run at 5 s is 51 840.
const MAX_POINTS: i64 = 10_000;

/// The shipped alert rules, from the repository or the Bazel runfiles.
fn alert_rules_source() -> PathBuf { manifest_dir().join("../../docs/operations/alert-rules.yaml") }

/// The port of a `http://host:port/path` URL.
pub(crate) fn url_port(url: &str) -> Result<u16, String> {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split('/').next().unwrap_or(rest);
    authority
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .ok_or_else(|| format!("no port in {url}"))
}

/// `prometheus.yml` for one job that scrapes each `(node id, metrics port)`
/// at `host.docker.internal`, labelled with its node id, and evaluates the
/// rules copy.
pub(crate) fn prometheus_config(targets: &[(i32, u16)]) -> String {
    let header = format!(
        "global:\n  scrape_interval: {INTERVAL_SECS}s\n  scrape_timeout: \
         {SCRAPE_TIMEOUT_SECS}s\n  evaluation_interval: {INTERVAL_SECS}s\nrule_files:\n  - \
         {CONTAINER_CONFIG_DIR}/{RULES_FILE}\nscrape_configs:\n  - job_name: {JOB}\n    \
         static_configs:\n"
    );
    let static_configs = targets.iter().map(|(node, port)| {
        format!(
            "      - targets: [\"host.docker.internal:{port}\"]\n        labels:\n          \
             node: \"{node}\"\n"
        )
    });
    std::iter::once(header).chain(static_configs).collect()
}

/// The host port of a `docker port` line such as `127.0.0.1:32768`.
pub(crate) fn published_port(output: &str) -> Result<u16, String> {
    output
        .lines()
        .find_map(|line| line.trim().rsplit_once(':')?.1.parse().ok())
        .ok_or_else(|| format!("no host port in `docker port` output {output:?}"))
}

/// Whether a raw HTTP response is a `200`.
pub(crate) fn is_ok_response(response: &str) -> bool {
    response
        .split_whitespace()
        .nth(1)
        .is_some_and(|status| status == "200")
        && response.starts_with("HTTP/")
}

/// Unix milliseconds as the decimal seconds the Prometheus API takes.
pub(crate) fn api_seconds(ms: i64) -> String {
    format!("{}.{:03}", ms.div_euclid(1000), ms.rem_euclid(1000))
}

/// The decimal seconds the Prometheus API returns, as Unix milliseconds.
/// Digits past the millisecond are dropped.
pub(crate) fn api_millis(seconds: &str) -> Option<i64> {
    let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    let whole: i64 = whole.parse().ok()?;
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let millis: String = fraction.chars().chain("000".chars()).take(3).collect();
    Some(whole * 1000 + millis.parse::<i64>().ok()?)
}

/// The `[start, end]` ranges, each at most [`MAX_POINTS`] steps, that cover
/// `[start_ms, end_ms]` at `step_ms` without overlap.
pub(crate) fn query_ranges(start_ms: i64, end_ms: i64, step_ms: i64) -> Vec<(i64, i64)> {
    let mut ranges = Vec::new();
    let mut from = start_ms;
    while from <= end_ms {
        let to = (from + (MAX_POINTS - 1) * step_ms).min(end_ms);
        ranges.push((from, to));
        from = to + step_ms;
    }
    ranges
}

/// A rule state Prometheus records in `ALERTS{alertstate}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AlertState {
    Pending,
    Firing,
}

/// One stretch of an alert instance in one state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Interval {
    pub(crate) state: AlertState,
    /// The first evaluation that saw the state.
    pub(crate) from_ms: i64,
    /// The first evaluation step that no longer saw it, or `None` when the
    /// last step of the range still did.
    pub(crate) to_ms: Option<i64>,
}

/// One alert instance and its intervals, in time order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AlertSeries {
    pub(crate) alertname: String,
    pub(crate) severity: Option<String>,
    /// Every other label of the instance: `instance`, `job`, `node`, and any
    /// the rule's expression keeps.
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) intervals: Vec<Interval>,
}

impl AlertSeries {
    /// Milliseconds spent firing, counting an open interval up to `end_ms`.
    pub(crate) fn firing_ms(&self, end_ms: i64) -> i64 {
        self.intervals
            .iter()
            .filter(|interval| interval.state == AlertState::Firing)
            .map(|interval| interval.to_ms.unwrap_or(end_ms) - interval.from_ms)
            .sum()
    }
}

/// One alerting rule as Prometheus loaded it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LoadedRule {
    pub(crate) group: String,
    pub(crate) alert: String,
    /// `ok`, `err`, or `unknown` before the first evaluation.
    pub(crate) health: String,
    pub(crate) last_error: Option<String>,
}

/// `alerts.json`: the rules Prometheus loaded and what each did over the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AlertHistory {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) step_ms: i64,
    pub(crate) rules: Vec<LoadedRule>,
    pub(crate) alerts: Vec<AlertSeries>,
}

impl AlertHistory {
    /// A summary of the loaded rules, one line per rule whose health is not
    /// `ok`, and one per alert instance that fired.
    pub(crate) fn lines(&self) -> Vec<String> {
        let unhealthy: Vec<&LoadedRule> = self
            .rules
            .iter()
            .filter(|rule| rule.health != "ok")
            .collect();
        let mut lines = vec![format!(
            "alert rules: {} loaded, {} not ok",
            self.rules.len(),
            unhealthy.len()
        )];
        lines.extend(unhealthy.iter().map(|rule| {
            format!(
                "rule {} (group {}) health {}: {}",
                rule.alert,
                rule.group,
                rule.health,
                rule.last_error.as_deref().unwrap_or("no error recorded")
            )
        }));
        for alert in &self.alerts {
            let firing: Vec<&Interval> = alert
                .intervals
                .iter()
                .filter(|interval| interval.state == AlertState::Firing)
                .collect();
            let (Some(first), Some(last)) = (firing.first(), firing.last()) else {
                continue;
            };
            let labels: Vec<String> = alert
                .labels
                .iter()
                .filter(|(name, _)| name.as_str() != "job")
                .map(|(name, value)| format!("{name}={value}"))
                .collect();
            let end = last.to_ms.map_or_else(
                || format!("still firing at the end ({})", self.offset(self.end_ms)),
                |to| format!("resolved at {}", self.offset(to)),
            );
            lines.push(format!(
                "{} {} severity={}: first fired at {}, {end}, firing {} in {} interval(s)",
                alert.alertname,
                labels.join(" "),
                alert.severity.as_deref().unwrap_or("none"),
                self.offset(first.from_ms),
                span(alert.firing_ms(self.end_ms)),
                firing.len()
            ));
        }
        lines
    }

    fn offset(&self, ms: i64) -> String { format!("+{}", span(ms - self.start_ms)) }
}

/// Milliseconds as `1h02m03s`.
pub(crate) fn span(ms: i64) -> String {
    let seconds = ms.max(0) / 1000;
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h{minutes:02}m{seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

/// The envelope of every Prometheus API answer.
#[derive(Deserialize)]
struct ApiResponse<T> {
    status: String,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn into_data(self) -> Result<T, String> {
        match (self.status.as_str(), self.data) {
            ("success", Some(data)) => Ok(data),
            _ => Err(format!(
                "Prometheus answered {}: {}",
                self.status,
                self.error.unwrap_or_default()
            )),
        }
    }
}

#[derive(Deserialize)]
struct Matrix {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<MatrixSeries>,
}

#[derive(Deserialize)]
struct MatrixSeries {
    metric: BTreeMap<String, String>,
    values: Vec<(serde_json::Number, String)>,
}

#[derive(Deserialize)]
struct RuleGroups {
    groups: Vec<RuleGroup>,
}

#[derive(Deserialize)]
struct RuleGroup {
    name: String,
    rules: Vec<Rule>,
}

#[derive(Deserialize)]
struct Rule {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    health: String,
    #[serde(rename = "lastError", default)]
    last_error: Option<String>,
}

/// The sample timestamps of every `ALERTS` series, keyed by its labels.
pub(crate) type Samples = BTreeMap<BTreeMap<String, String>, Vec<i64>>;

/// Add the series of one `query_range` answer to `samples`.
pub(crate) fn add_matrix(samples: &mut Samples, body: serde_json::Value) -> Result<(), String> {
    let response: ApiResponse<Matrix> =
        serde_json::from_value(body).map_err(|error| format!("query_range answer: {error}"))?;
    let matrix = response.into_data()?;
    if matrix.result_type != "matrix" {
        return Err(format!("query_range answered a {}", matrix.result_type));
    }
    for series in matrix.result {
        let times = series
            .values
            .iter()
            .map(|(time, _)| {
                api_millis(&time.to_string()).ok_or_else(|| format!("sample time {time}"))
            })
            .collect::<Result<Vec<i64>, String>>()?;
        samples.entry(series.metric).or_default().extend(times);
    }
    Ok(())
}

/// An alert instance: its name, its severity, and its other labels.
type AlertKey = (String, Option<String>, BTreeMap<String, String>);

/// Fold `samples`, evaluated every `step_ms` from `start_ms` to `end_ms`,
/// into one [`AlertSeries`] per alert instance. A gap of more than one step
/// ends an interval.
pub(crate) fn fold(samples: Samples, start_ms: i64, end_ms: i64, step_ms: i64) -> Vec<AlertSeries> {
    let last_step = start_ms + (end_ms - start_ms).max(0) / step_ms * step_ms;
    let mut alerts: BTreeMap<AlertKey, Vec<Interval>> = BTreeMap::new();
    for (mut labels, mut times) in samples {
        labels.remove("__name__");
        let state = match labels.remove("alertstate").as_deref() {
            Some("firing") => AlertState::Firing,
            _ => AlertState::Pending,
        };
        let alertname = labels.remove("alertname").unwrap_or_default();
        let severity = labels.remove("severity");
        times.sort_unstable();
        times.dedup();
        let intervals = alerts.entry((alertname, severity, labels)).or_default();
        let mut runs: Vec<(i64, i64)> = Vec::new();
        for time in times {
            match runs.last_mut() {
                Some((_, last)) if time - *last <= step_ms => *last = time,
                _ => runs.push((time, time)),
            }
        }
        intervals.extend(runs.into_iter().map(|(from_ms, last)| Interval {
            state,
            from_ms,
            to_ms: (last < last_step).then_some(last + step_ms),
        }));
    }
    alerts
        .into_iter()
        .map(|((alertname, severity, labels), mut intervals)| {
            intervals.sort_by_key(|interval| (interval.from_ms, interval.state));
            AlertSeries {
                alertname,
                severity,
                labels,
                intervals,
            }
        })
        .collect()
}

/// The alerting rules of a `/api/v1/rules` answer.
pub(crate) fn loaded_rules(body: serde_json::Value) -> Result<Vec<LoadedRule>, String> {
    let response: ApiResponse<RuleGroups> =
        serde_json::from_value(body).map_err(|error| format!("rules answer: {error}"))?;
    Ok(response
        .into_data()?
        .groups
        .into_iter()
        .flat_map(|group| {
            group
                .rules
                .into_iter()
                .filter(|rule| rule.kind == "alerting")
                .map(move |rule| LoadedRule {
                    group: group.name.clone(),
                    alert: rule.name,
                    health: rule.health,
                    last_error: rule.last_error.filter(|error| !error.is_empty()),
                })
        })
        .collect())
}

/// A Prometheus container that scrapes the cluster and evaluates the shipped
/// alert rules. Dropping it removes the container.
pub(crate) struct AlertSidecar {
    name: String,
    /// `http://127.0.0.1:<port>`, the API on the host.
    url: String,
    /// The configuration directory the container mounts. Kept until the
    /// container is gone.
    _config: TempDir,
}

impl AlertSidecar {
    /// Start Prometheus against every node of `nodes`, and wait until it is
    /// ready. The configuration it runs is also written to `dir`.
    pub(crate) fn start(nodes: &Nodes, dir: &Path) -> Result<Self, String> {
        let targets = nodes
            .ids()
            .into_iter()
            .filter_map(|id| nodes.node(id))
            .map(|node| url_port(&node.metrics_url).map(|port| (node.id, port)))
            .collect::<Result<Vec<_>, String>>()?;
        let config = prometheus_config(&targets);
        let rules_source = alert_rules_source();
        let rules = std::fs::read_to_string(&rules_source)
            .map_err(|error| format!("read {}: {error}", rules_source.display()))?;
        let config_dir = tempfile::Builder::new()
            .prefix("soak-prometheus-")
            .tempdir()
            .map_err(|error| format!("create the Prometheus directory: {error}"))?;
        // The image runs as `nobody`, which must read what the test wrote.
        for (name, text) in [(CONFIG_FILE, &config), (RULES_FILE, &rules)] {
            let path = config_dir.path().join(name);
            std::fs::write(&path, text)
                .and_then(|()| std::fs::set_permissions(&path, Permissions::from_mode(0o644)))
                .map_err(|error| format!("write {}: {error}", path.display()))?;
        }
        std::fs::set_permissions(config_dir.path(), Permissions::from_mode(0o755))
            .map_err(|error| format!("open the Prometheus directory: {error}"))?;
        std::fs::write(dir.join(CONFIG_FILE), &config)
            .map_err(|error| format!("write {CONFIG_FILE} evidence: {error}"))?;

        let name = format!("krabka-soak-prometheus-{}-{}", std::process::id(), now_ms());
        let mount = format!("{}:{CONTAINER_CONFIG_DIR}:ro", config_dir.path().display());
        let config_flag = format!("--config.file={CONTAINER_CONFIG_DIR}/{CONFIG_FILE}");
        let retention = format!("--storage.tsdb.retention.time={RETENTION}");
        docker_try(&[
            "run",
            "--detach",
            "--name",
            &name,
            "--add-host",
            "host.docker.internal:host-gateway",
            "--publish",
            "127.0.0.1::9090",
            "--volume",
            &mount,
            PROMETHEUS_IMAGE,
            &config_flag,
            "--storage.tsdb.path=/prometheus",
            &retention,
        ])?;
        let mut sidecar = Self {
            name,
            url: String::new(),
            _config: config_dir,
        };
        let port = published_port(&docker_try(&["port", &sidecar.name, "9090/tcp"])?)?;
        sidecar.url = format!("http://127.0.0.1:{port}");
        sidecar.wait_ready(port)?;
        Ok(sidecar)
    }

    /// Poll `/-/ready` until it answers `200`. A blocking read, so `start`
    /// stays synchronous like the cluster it runs beside.
    fn wait_ready(&self, port: u16) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last = String::new();
        while Instant::now() < deadline {
            match ready_once(port) {
                Ok(response) if is_ok_response(&response) => return Ok(()),
                Ok(response) => last = response,
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        let logs = docker_try(&["logs", "--tail", "20", &self.name]).unwrap_or_else(|e| e);
        Err(format!(
            "Prometheus {} not ready within {READY_TIMEOUT:?}: {last}\n{logs}",
            self.url
        ))
    }

    /// The alert history from `start_ms` to `end_ms`, and the rules loaded.
    pub(crate) async fn history(
        &self,
        http: &reqwest::Client,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<AlertHistory, String> {
        let step_ms = INTERVAL_SECS * 1000;
        let mut samples = Samples::new();
        for (from, to) in query_ranges(start_ms, end_ms, step_ms) {
            let url = format!(
                "{}/api/v1/query_range?query=ALERTS&start={}&end={}&step={INTERVAL_SECS}s",
                self.url,
                api_seconds(from),
                api_seconds(to)
            );
            add_matrix(&mut samples, get_json(http, &url).await?)?;
        }
        let rules = loaded_rules(get_json(http, &format!("{}/api/v1/rules", self.url)).await?)?;
        Ok(AlertHistory {
            start_ms,
            end_ms,
            step_ms,
            rules,
            alerts: fold(samples, start_ms, end_ms, step_ms),
        })
    }

    /// Write the history so far to `dir/alerts.json`, and return it. A
    /// failed read leaves the last checkpoint in place.
    pub(crate) async fn checkpoint(
        &self,
        http: &reqwest::Client,
        start_ms: i64,
        now_ms: i64,
        dir: &Path,
    ) -> Result<AlertHistory, String> {
        let history = self.history(http, start_ms, now_ms).await?;
        write_json(dir, EVIDENCE_FILE, &history);
        Ok(history)
    }
}

impl Drop for AlertSidecar {
    fn drop(&mut self) {
        // Best effort: a panic in `drop` would replace the real failure.
        remove_container_with_volumes(&self.name);
    }
}

/// One `GET /-/ready`, answered or not.
fn ready_once(port: u16) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|error| error.to_string())?;
    let timeout = Some(Duration::from_secs(2));
    stream
        .set_read_timeout(timeout)
        .and_then(|()| stream.set_write_timeout(timeout))
        .and_then(|()| stream.write_all(b"GET /-/ready HTTP/1.0\r\nHost: localhost\r\n\r\n"))
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    // Docker's proxy accepts and closes before Prometheus listens, which
    // reads as an empty answer.
    let _ = stream.read_to_string(&mut response);
    Ok(response)
}

/// `url`'s JSON body.
async fn get_json(http: &reqwest::Client, url: &str) -> Result<serde_json::Value, String> {
    http.get(url)
        .send()
        .await
        .map_err(|error| format!("GET {url}: {error}"))?
        .json()
        .await
        .map_err(|error| format!("GET {url}: {error}"))
}

#[cfg(test)]
mod tests {
    use {
        super::{
            AlertHistory, AlertSeries, AlertState, Interval, LoadedRule, Samples, add_matrix,
            api_millis, api_seconds, fold, is_ok_response, loaded_rules, prometheus_config,
            published_port, query_ranges, span, url_port,
        },
        assert2::{assert, check},
        serde_json::json,
        std::collections::BTreeMap,
    };

    const STEP: i64 = 5_000;

    #[yare::parameterized(
        loopback = { "http://127.0.0.1:40123/metrics", Ok(40123) },
        no_path = { "http://host.docker.internal:9404", Ok(9404) },
        no_port = { "http://127.0.0.1/metrics", Err(()) },
    )]
    fn a_target_port_comes_from_the_metrics_url(url: &str, port: Result<u16, ()>) {
        check!(url_port(url).map_err(|_| ()) == port);
    }

    #[test]
    fn the_config_scrapes_every_node_as_the_job_the_rules_name() {
        check!(
            prometheus_config(&[(1, 40001), (2, 40002)])
                == "global:
  scrape_interval: 5s
  scrape_timeout: 4s
  evaluation_interval: 5s
rule_files:
  - /etc/prometheus/soak/alert-rules.yaml
scrape_configs:
  - job_name: krabka-broker
    static_configs:
      - targets: [\"host.docker.internal:40001\"]
        labels:
          node: \"1\"
      - targets: [\"host.docker.internal:40002\"]
        labels:
          node: \"2\"
"
        );
    }

    #[yare::parameterized(
        ipv4 = { "127.0.0.1:32768\n", Ok(32768) },
        both_families = { "0.0.0.0:32769\n[::]:32769\n", Ok(32769) },
        empty = { "", Err(()) },
    )]
    fn the_api_port_is_the_published_one(output: &str, port: Result<u16, ()>) {
        check!(published_port(output).map_err(|_| ()) == port);
    }

    #[yare::parameterized(
        ready = { "HTTP/1.0 200 OK\r\n\r\nPrometheus Server is Ready.\n", true },
        starting = { "HTTP/1.0 503 Service Unavailable\r\n\r\n", false },
        closed_by_proxy = { "", false },
    )]
    fn ready_is_a_200(response: &str, ready: bool) {
        check!(is_ok_response(response) == ready);
    }

    #[yare::parameterized(
        whole = { 1_700_000_000_000, "1700000000.000" },
        millis = { 1_700_000_000_042, "1700000000.042" },
    )]
    fn milliseconds_go_out_as_seconds(ms: i64, seconds: &str) {
        check!(api_seconds(ms) == seconds);
        check!(api_millis(seconds) == Some(ms));
    }

    #[yare::parameterized(
        integer = { "1700000000", Some(1_700_000_000_000) },
        one_digit = { "1700000000.5", Some(1_700_000_000_500) },
        past_millis = { "1700000000.12345", Some(1_700_000_000_123) },
        exponent = { "1.7e9", None },
    )]
    fn seconds_come_back_as_milliseconds(seconds: &str, ms: Option<i64>) {
        check!(api_millis(seconds) == ms);
    }

    #[yare::parameterized(
        one_range = { 0, 10 * STEP, vec![(0, 10 * STEP)] },
        a_single_point = { 0, 0, vec![(0, 0)] },
        split_at_the_point_limit = { 0, 15_000 * STEP, vec![(0, 9_999 * STEP), (10_000 * STEP, 15_000 * STEP)] },
    )]
    fn a_long_run_is_queried_in_ranges_prometheus_accepts(
        start: i64,
        end: i64,
        ranges: Vec<(i64, i64)>,
    ) {
        check!(query_ranges(start, end, STEP) == ranges);
    }

    fn matrix(series: &[(&str, &str, &[i64])]) -> serde_json::Value {
        let result: Vec<serde_json::Value> = series
            .iter()
            .map(|(alert, state, times)| {
                let values: Vec<serde_json::Value> =
                    times.iter().map(|seconds| json!([seconds, "1"])).collect();
                json!({
                    "metric": {
                        "__name__": "ALERTS",
                        "alertname": alert,
                        "alertstate": state,
                        "instance": "host.docker.internal:40002",
                        "job": "krabka-broker",
                        "node": "2",
                        "severity": "critical",
                    },
                    "values": values,
                })
            })
            .collect();
        json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
    }

    fn labels() -> BTreeMap<String, String> {
        [
            ("instance", "host.docker.internal:40002"),
            ("job", "krabka-broker"),
            ("node", "2"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
    }

    fn interval(state: AlertState, from_s: i64, to_s: Option<i64>) -> Interval {
        Interval {
            state,
            from_ms: from_s * 1000,
            to_ms: to_s.map(|to| to * 1000),
        }
    }

    fn folded(series: &[(&str, &str, &[i64])], end_s: i64) -> Vec<AlertSeries> {
        let mut samples = Samples::new();
        assert!(let Ok(()) = add_matrix(&mut samples, matrix(series)));
        fold(samples, 0, end_s * 1000, STEP)
    }

    #[yare::parameterized(
        pending_then_firing_then_resolved = {
            &[("KrabkaBrokerDown", "pending", &[100, 105, 110]), ("KrabkaBrokerDown", "firing", &[115, 120])],
            200,
            vec![
                interval(AlertState::Pending, 100, Some(115)),
                interval(AlertState::Firing, 115, Some(125)),
            ]
        },
        still_firing_at_the_last_step = {
            &[("KrabkaBrokerDown", "firing", &[190, 195, 200])],
            202,
            vec![interval(AlertState::Firing, 190, None)]
        },
        a_gap_is_two_intervals = {
            &[("KrabkaBrokerDown", "pending", &[10, 15, 40])],
            200,
            vec![
                interval(AlertState::Pending, 10, Some(20)),
                interval(AlertState::Pending, 40, Some(45)),
            ]
        },
    )]
    fn alerts_fold_into_intervals_per_state(
        series: &[(&str, &str, &[i64])],
        end_s: i64,
        intervals: Vec<Interval>,
    ) {
        check!(
            folded(series, end_s)
                == vec![AlertSeries {
                    alertname: "KrabkaBrokerDown".to_owned(),
                    severity: Some("critical".to_owned()),
                    labels: labels(),
                    intervals,
                }]
        );
    }

    #[test]
    fn ranges_of_one_series_join_into_one_interval() {
        let mut samples = Samples::new();
        assert!(
            let Ok(()) = add_matrix(&mut samples, matrix(&[("KrabkaBrokerDown", "firing", &[100, 105])]))
        );
        assert!(
            let Ok(()) = add_matrix(&mut samples, matrix(&[("KrabkaBrokerDown", "firing", &[110])]))
        );
        let alerts = fold(samples, 0, 200_000, STEP);
        check!(alerts.len() == 1);
        check!(alerts[0].intervals == vec![interval(AlertState::Firing, 100, Some(115))]);
    }

    #[yare::parameterized(
        an_error = { json!({"status": "error", "errorType": "bad_data", "error": "exceeded maximum resolution"}) },
        a_vector = { json!({"status": "success", "data": {"resultType": "vector", "result": []}}) },
        not_an_answer = { json!({"nothing": true}) },
    )]
    fn a_bad_answer_is_an_error(body: serde_json::Value) {
        check!(add_matrix(&mut Samples::new(), body).is_err());
    }

    #[test]
    fn only_alerting_rules_are_recorded_with_their_health() {
        let body = json!({"status": "success", "data": {"groups": [{
            "name": "krabka-broker-cluster",
            "file": "/etc/prometheus/soak/alert-rules.yaml",
            "rules": [
                {"name": "KrabkaBrokerDown", "type": "alerting", "health": "ok", "lastError": ""},
                {"name": "KrabkaBad", "type": "alerting", "health": "err", "lastError": "many-to-many matching not allowed"},
                {"name": "krabka:rate", "type": "recording", "health": "ok"},
            ],
        }]}});
        let rule = |alert: &str, health: &str, last_error: Option<&str>| LoadedRule {
            group: "krabka-broker-cluster".to_owned(),
            alert: alert.to_owned(),
            health: health.to_owned(),
            last_error: last_error.map(str::to_owned),
        };
        check!(
            loaded_rules(body)
                == Ok(vec![
                    rule("KrabkaBrokerDown", "ok", None),
                    rule(
                        "KrabkaBad",
                        "err",
                        Some("many-to-many matching not allowed")
                    ),
                ])
        );
    }

    #[test]
    fn the_lines_name_unhealthy_rules_and_alerts_that_fired() {
        let history = AlertHistory {
            start_ms: 0,
            end_ms: 7_200_000,
            step_ms: STEP,
            rules: vec![
                LoadedRule {
                    group: "g".to_owned(),
                    alert: "KrabkaBrokerDown".to_owned(),
                    health: "ok".to_owned(),
                    last_error: None,
                },
                LoadedRule {
                    group: "g".to_owned(),
                    alert: "KrabkaBad".to_owned(),
                    health: "err".to_owned(),
                    last_error: Some("boom".to_owned()),
                },
            ],
            alerts: vec![
                AlertSeries {
                    alertname: "KrabkaBrokerDown".to_owned(),
                    severity: Some("critical".to_owned()),
                    labels: labels(),
                    intervals: vec![
                        interval(AlertState::Pending, 60, Some(180)),
                        interval(AlertState::Firing, 180, Some(300)),
                        interval(AlertState::Firing, 3_600, None),
                    ],
                },
                AlertSeries {
                    alertname: "KrabkaMetadataLag".to_owned(),
                    severity: Some("warning".to_owned()),
                    labels: labels(),
                    intervals: vec![interval(AlertState::Pending, 60, Some(90))],
                },
            ],
        };
        check!(
            history.lines()
                == vec![
                    "alert rules: 2 loaded, 1 not ok".to_owned(),
                    "rule KrabkaBad (group g) health err: boom".to_owned(),
                    "KrabkaBrokerDown instance=host.docker.internal:40002 node=2 \
                     severity=critical: first fired at +3m00s, still firing at the end \
                     (+2h00m00s), firing 1h02m00s in 2 interval(s)"
                        .to_owned(),
                ]
        );
    }

    #[yare::parameterized(
        seconds = { 42_000, "42s" },
        minutes = { 125_000, "2m05s" },
        hours = { 72 * 3_600_000 + 61_000, "72h01m01s" },
    )]
    fn a_span_reads_in_hours_minutes_and_seconds(ms: i64, text: &str) {
        check!(span(ms) == text);
    }
}
