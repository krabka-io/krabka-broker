//! `krabka-format` against `kafka-storage format`, through the binary.
//!
//! Each case runs the binary as a subprocess and reads back what it wrote and
//! printed. The Kafka behaviour each case pins was observed with
//! `kafka-storage format` on `apache/kafka:4.3.1`; `docs/format-divergences.md`
//! lists the places krabka differs on purpose.
#![cfg(test)]

use {
    krabka_broker::bootstrap,
    krabka_format::{ClusterId, DirectoryId, FAIL_AFTER_ENV, META_PROPERTIES, MetaProperties},
    krabka_metadata::MetadataRecord,
    std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
        process::{Command, Output},
    },
};

/// The node id of every run, as `--node-id` takes it and as
/// `meta.properties` records it.
const NODE: &str = "1";
const NODE_ID: i32 = 1;

const STANDALONE: &[&str] = &[
    "--standalone",
    "--node-id",
    NODE,
    "--controller-listener",
    "controller-1:9093",
];

/// The shared cluster id in Kafka's form.
const CLUSTER_ID: &str = "AQIDBAUGBwgJCgsMDQ4PEA";

fn krabka_format(args: &[&str], fault: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_krabka-format"));
    command.args(args).env_remove(FAIL_AFTER_ENV);
    if let Some(name) = fault {
        command.env(FAIL_AFTER_ENV, name);
    }
    command.output().expect("run krabka-format")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf-8 stdout")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("utf-8 stderr")
}

fn path_str(path: &Path) -> &str { path.to_str().expect("utf-8 path") }

/// The ids in a directory's `meta.properties`, as written.
fn written_ids(dir: &Path) -> MetaProperties {
    MetaProperties::read(dir)
        .expect("meta.properties reads")
        .expect("meta.properties exists")
}

/// Kafka's form of a directory's cluster id.
fn written_cluster_id(dir: &Path) -> String { written_ids(dir).cluster_id.to_string() }

/// Kafka's form of a directory's own id.
fn written_directory_id(dir: &Path) -> String {
    written_ids(dir)
        .directory_id
        .expect("directory.id")
        .to_string()
}

fn manifest_cluster_id(dir: &Path) -> String {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.join("bootstrap.json")).expect("manifest"))
            .expect("manifest is json");
    manifest["cluster_id"]
        .as_str()
        .expect("cluster_id")
        .to_owned()
}

/// Kafka's bootstrap snapshot path, `Snapshots.BOOTSTRAP_SNAPSHOT_ID` in the
/// `__cluster_metadata-0` partition directory.
fn checkpoint(dir: &Path) -> PathBuf {
    dir.join("__cluster_metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

krabka_macros::directory_tree_fixture!(
    snapshot,
    (PathBuf, Vec<u8>),
    { /// Every file under `dir` with its bytes, for an unchanged-on-rerun check.
    },
    (|_dir: &Path, path: &Path| (path.to_path_buf(), fs::read(path).expect("read")))
);

struct FormatBinaryScenario {
    root: tempfile::TempDir,
    log_dirs: Vec<String>,
    metadata: Option<String>,
    spelling: String,
    cluster: Option<String>,
    node: String,
    flags: Vec<String>,
    output: Option<Output>,
    initial: Option<Output>,
    initial_ids: Option<MetaProperties>,
    before: Vec<(PathBuf, Vec<u8>)>,
    seed_records: Option<usize>,
}
#[rstest::fixture]
fn format_binary() -> FormatBinaryScenario {
    let root = tempfile::tempdir().expect("format binary root");
    FormatBinaryScenario {
        root,
        log_dirs: vec!["data".into()],
        metadata: None,
        spelling: "repeated".into(),
        cluster: Some(CLUSTER_ID.into()),
        node: NODE.into(),
        flags: Vec::new(),
        output: None,
        initial: None,
        initial_ids: None,
        before: Vec::new(),
        seed_records: None,
    }
}
impl FormatBinaryScenario {
    fn argv(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        if let Some(metadata) = &self.metadata {
            arguments.extend([
                "--metadata-log-dir".into(),
                self.root.path().join(metadata).display().to_string(),
            ]);
        }
        let paths: Vec<String> = self
            .log_dirs
            .iter()
            .map(|directory| self.root.path().join(directory).display().to_string())
            .collect();
        if self.spelling == "comma-separated" {
            arguments.extend(["--log-dir".into(), paths.join(",")]);
        } else {
            for directory in paths {
                arguments.extend(["--log-dir".into(), directory]);
            }
        }
        if let Some(cluster) = &self.cluster {
            arguments.extend(["--cluster-id".into(), cluster.clone()]);
        }
        arguments.extend(["--node-id".into(), self.node.clone()]);
        arguments.extend(self.flags.clone());
        arguments
    }

    fn run(&self, fault: Option<&str>) -> Output {
        let arguments = self.argv();
        let borrowed: Vec<&str> = arguments.iter().map(String::as_str).collect();
        krabka_format(&borrowed, fault)
    }

    fn output(&self) -> &Output { self.output.as_ref().expect("format binary ran") }
}
#[rstest_bdd_macros::given("format cluster ID argument {input}")]
fn binary_cluster_argument(format_binary: &mut FormatBinaryScenario, input: String) {
    format_binary.cluster = Some(input);
    format_binary.flags = vec!["--no-initial-controllers".into()];
}
#[rstest_bdd_macros::given("format directory identity options {options}")]
fn binary_directory_argument(format_binary: &mut FormatBinaryScenario, options: String) {
    format_binary.flags = options.split_whitespace().map(str::to_owned).collect();
}
#[rstest_bdd_macros::when("the format binary executes")]
fn execute_format_binary(format_binary: &mut FormatBinaryScenario) {
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then("the cluster ID is stored and printed as {canonical}")]
fn binary_canonical_cluster(format_binary: &FormatBinaryScenario, canonical: String) {
    let output = format_binary.output();
    assert2::assert!(output.status.code() == Some(0), "{}", stderr(output));
    let directory = format_binary.root.path().join("data");
    let got = (
        written_cluster_id(&directory),
        manifest_cluster_id(&directory),
        stdout(output).contains(&format!("with cluster-id {canonical} ")),
    );
    assert2::assert!(got == (canonical.clone(), canonical, true));
}
#[rstest_bdd_macros::then(
    "the malformed cluster ID exits with usage error before writing a directory"
)]
fn binary_rejected_cluster(format_binary: &FormatBinaryScenario) {
    assert2::assert!(format_binary.output().status.code() == Some(2));
    assert2::assert!(!format_binary.root.path().join("data").exists());
}
#[rstest_bdd_macros::then("the broker reads directory UUID 100 in Kafka form")]
fn binary_canonical_directory(format_binary: &FormatBinaryScenario) {
    assert2::assert!(
        format_binary.output().status.code() == Some(0),
        "{}",
        stderr(format_binary.output())
    );
    let directory = format_binary.root.path().join("data");
    assert2::assert!(
        bootstrap::read_meta_properties(&directory).ok()
            == Some(MetaProperties {
                cluster_id: CLUSTER_ID.parse().expect("cluster id"),
                node_id: NODE_ID,
                directory_id: Some(DirectoryId(uuid::Uuid::from_u128(100)))
            })
    );
    assert2::assert!(written_directory_id(&directory) == "AAAAAAAAAAAAAAAAAAAAZA");
}
#[rstest_bdd_macros::given(
    "a standalone format of log directories {directories} using {spelling} flags and metadata {metadata}"
)]
fn binary_directory_layout(
    format_binary: &mut FormatBinaryScenario,
    directories: String,
    spelling: String,
    metadata: String,
) {
    format_binary.log_dirs = directories.split(',').map(str::to_owned).collect();
    format_binary.metadata = (metadata != "first-log-dir").then_some(metadata);
    format_binary.spelling = spelling;
    format_binary.flags = vec![
        "--standalone".into(),
        "--controller-listener".into(),
        "controller-1:9093".into(),
    ];
}
#[rstest_bdd_macros::given("a reference standalone bootstrap record count")]
fn binary_seed_baseline(format_binary: &mut FormatBinaryScenario) {
    format_binary.seed_records = Some(standalone_seed_records(format_binary.root.path()));
}
#[rstest_bdd_macros::then(
    "the binary reports and writes these directories in metadata-first order"
)]
fn binary_ordered_directories(format_binary: &FormatBinaryScenario, datatable: Vec<Vec<String>>) {
    let output = format_binary.output();
    assert2::assert!(output.status.code() == Some(0), "{}", stderr(output));
    let seed_records = format_binary
        .seed_records
        .expect("standalone baseline given");
    let rows: Vec<(PathBuf, bool)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [directory, kind] = row.as_slice() else {
                panic!("directory and kind columns required")
            };
            (
                format_binary.root.path().join(directory),
                match kind.as_str() {
                    "metadata-voter" => true,
                    "data" => false,
                    _ => panic!("unknown directory kind {kind}"),
                },
            )
        })
        .collect();
    let got: Vec<Formatted> = rows
        .iter()
        .map(|(directory, _)| formatted(directory))
        .collect();
    let expected: Vec<Formatted> = rows
        .iter()
        .map(|(_, voter)| {
            if *voter {
                metadata_directory(true, seed_records)
            } else {
                data_directory()
            }
        })
        .collect();
    assert2::assert!(got == expected);
    let mut lines: Vec<String> = rows
        .iter()
        .map(|(directory, voter)| {
            format!(
                "Formatting {} {} with metadata.version 4.3-IV0.",
                if *voter {
                    "dynamic metadata voter directory"
                } else {
                    "data directory"
                },
                directory.display()
            )
        })
        .collect();
    lines.push(format!(
        "Formatted {} log directories with cluster-id {CLUSTER_ID} ({seed_records} seed record(s))",
        rows.len()
    ));
    assert2::assert!(stdout(output).lines().collect::<Vec<_>>() == lines);
    let ids: BTreeSet<String> = rows
        .iter()
        .map(|(directory, _)| written_directory_id(directory))
        .collect();
    assert2::assert!(ids.len() == rows.len(), "{ids:?}");
    for (directory, voter) in &rows {
        let expected = if *voter {
            vec![
                "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
                "bootstrap.json",
                "bootstrap.records.bin",
                META_PROPERTIES,
            ]
        } else {
            vec![META_PROPERTIES]
        };
        assert2::assert!(files_under(directory) == expected);
    }
}
#[rstest_bdd_macros::given("directory a is formatted and directory b is absent")]
fn one_formatted_directory(format_binary: &mut FormatBinaryScenario) {
    format_binary.log_dirs = vec!["a".into()];
    format_binary.initial = Some(format_binary.run(None));
    format_binary.initial_ids = Some(written_ids(&format_binary.root.path().join("a")));
    format_binary.log_dirs = vec!["a".into(), "b".into()];
    format_binary.spelling = "comma-separated".into();
}
fn check_format_refusal(output: &Output, message: String) {
    assert2::assert!((output.status.code(), stderr(output)) == (Some(3), message));
}
#[rstest_bdd_macros::then("the existing directory refuses the run and directory b remains absent")]
fn refused_existing_directory(format_binary: &FormatBinaryScenario) {
    assert2::assert!(
        format_binary
            .initial
            .as_ref()
            .expect("initial format ran")
            .status
            .success()
    );
    let output = format_binary.output();
    let directory = format_binary.root.path().join("a");
    check_format_refusal(
        output,
        format!(
            "Log directory {} is already formatted. Use --ignore-formatted to ignore this directory and format the others.\n",
            directory.display()
        ),
    );
    assert2::assert!(!format_binary.root.path().join("b").exists());
}
#[rstest_bdd_macros::when("the format binary tries another cluster with ignore-formatted")]
fn mismatched_ignored_format(format_binary: &mut FormatBinaryScenario) {
    format_binary.cluster = Some("BQIDBAUGBwgJCgsMDQ4PEA".into());
    format_binary.flags = vec!["--ignore-formatted".into()];
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then(
    "the existing cluster ID mismatch is refused even when formatted directories are ignored"
)]
fn refused_ignored_cluster(format_binary: &FormatBinaryScenario) {
    let output = format_binary.output();
    let directory = format_binary.root.path().join("a");
    check_format_refusal(
        output,
        format!(
            "Invalid cluster.id in: {}. Expected BQIDBAUGBwgJCgsMDQ4PEA, but read {CLUSTER_ID}\n",
            directory.join(META_PROPERTIES).display()
        ),
    );
}
#[rstest_bdd_macros::when(
    "the format binary ignores formatted directories and discovers the cluster ID"
)]
fn ignore_and_discover_cluster(format_binary: &mut FormatBinaryScenario) {
    format_binary.cluster = None;
    format_binary.flags = vec!["--ignore-formatted".into()];
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then(
    "directory a keeps its identity and directory b joins the cluster with a distinct identity"
)]
fn mixed_directory_format(format_binary: &FormatBinaryScenario) {
    let output = format_binary.output();
    assert2::assert!(output.status.code() == Some(0), "{}", stderr(output));
    let a = format_binary.root.path().join("a");
    let b = format_binary.root.path().join("b");
    let a_ids = format_binary
        .initial_ids
        .expect("initial directory IDs recorded");
    assert2::assert!(written_ids(&a) == a_ids);
    let b_ids = written_ids(&b);
    assert2::assert!(
        MetaProperties {
            directory_id: None,
            ..b_ids
        } == MetaProperties {
            directory_id: None,
            ..a_ids
        }
    );
    assert2::assert!(b_ids.directory_id != a_ids.directory_id);
    assert2::assert!(stdout(output).contains(&format!(
        "Formatting data directory {} with metadata.version 4.3-IV0.",
        b.display()
    )));
}
#[rstest_bdd_macros::when("the format binary ignores the fully formatted directory set again")]
fn ignore_formatted_again(format_binary: &mut FormatBinaryScenario) {
    format_binary.before = snapshot(format_binary.root.path());
    format_binary.cluster = Some(CLUSTER_ID.into());
    format_binary.flags = vec!["--ignore-formatted".into()];
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then(
    "the already-formatted message is printed and every file remains unchanged"
)]
fn unchanged_ignored_format(format_binary: &FormatBinaryScenario) {
    let output = format_binary.output();
    assert2::assert!(output.status.code() == Some(0), "{}", stderr(output));
    assert2::assert!(
        stdout(output)
            .lines()
            .last()
            .is_some_and(|line| line == "All of the log directories are already formatted.")
    );
    assert2::assert!(snapshot(format_binary.root.path()) == format_binary.before);
}
#[rstest_bdd_macros::when("the format binary is interrupted after {artifact}")]
fn interrupt_format_binary(format_binary: &mut FormatBinaryScenario, artifact: String) {
    format_binary.output = Some(format_binary.run(Some(&artifact)));
}
#[rstest_bdd_macros::then("the injected interruption exits with code 4")]
fn injected_format_failure(format_binary: &FormatBinaryScenario) {
    let output = format_binary.output();
    assert2::assert!(output.status.code() == Some(4));
    assert2::assert!(stderr(output).contains("injected failure after"));
}
#[rstest_bdd_macros::when("the format binary is rerun without interruption")]
fn rerun_format_binary(format_binary: &mut FormatBinaryScenario) {
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then("the format binary exits with code {code}")]
fn format_binary_exit(format_binary: &FormatBinaryScenario, code: i32) {
    assert2::assert!(
        format_binary.output().status.code() == Some(code),
        "{}",
        stderr(format_binary.output())
    );
}
#[rstest_bdd_macros::when("the interrupted format is resumed with ignore-formatted")]
fn resume_published_format(format_binary: &mut FormatBinaryScenario) {
    format_binary.flags.push("--ignore-formatted".into());
    format_binary.output = Some(format_binary.run(None));
}
#[rstest_bdd_macros::then(
    "both interrupted-format directories contain complete Kafka artifacts with voters only in the checkpoint"
)]
fn resumed_format_artifacts(format_binary: &FormatBinaryScenario) {
    let metadata = format_binary.root.path().join("meta");
    let data = format_binary.root.path().join("data");
    for directory in [&metadata, &data] {
        let properties = bootstrap::read_meta_properties(directory)
            .unwrap_or_else(|error| panic!("{} does not read: {error}", directory.display()));
        assert2::assert!(
            (properties.cluster_id, properties.node_id)
                == (
                    ClusterId(uuid::Uuid::from_u128(
                        0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10
                    )),
                    NODE_ID
                )
        );
    }
    assert2::assert!(
        [files_under(&metadata), files_under(&data)]
            == [
                vec![
                    "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
                    "bootstrap.json",
                    "bootstrap.records.bin",
                    META_PROPERTIES
                ],
                vec![META_PROPERTIES]
            ]
    );
    let records = bootstrap::load_bootstrap_records(&metadata).expect("records");
    assert2::assert!(
        records
            .iter()
            .all(|record| !matches!(record, MetadataRecord::V1Voters(_)))
    );
}
#[rstest_bdd_macros::given(
    "metadata directory meta and log directory data for node 2147483647 with directory UUID 100"
)]
fn binary_property_layout(format_binary: &mut FormatBinaryScenario) {
    format_binary.metadata = Some("meta".into());
    format_binary.log_dirs = vec!["data".into()];
    format_binary.node = "2147483647".into();
    format_binary.flags = vec!["--directory-id".into(), "AAAAAAAAAAAAAAAAAAAAZA".into()];
}
#[rstest_bdd_macros::then(
    "every meta.properties file has Kafka's ordered fields and UTC date comment"
)]
fn binary_property_bytes(format_binary: &FormatBinaryScenario) {
    assert2::assert!(
        format_binary.output().status.code() == Some(0),
        "{}",
        stderr(format_binary.output())
    );
    let metadata = format_binary.root.path().join("meta");
    let data = format_binary.root.path().join("data");
    for (directory, id) in [
        (&metadata, "AAAAAAAAAAAAAAAAAAAAZA".to_owned()),
        (&data, written_directory_id(&data)),
    ] {
        let text = fs::read_to_string(directory.join(META_PROPERTIES)).expect("meta.properties");
        let mut lines: Vec<&str> = text.split_inclusive('\n').collect();
        let date = lines.get(1).map(|line| line.trim_end_matches('\n'));
        assert2::assert!(
            date.is_some_and(is_utc_date_comment),
            "{}: {date:?}",
            directory.display()
        );
        lines[1] = "#<date>\n";
        assert2::assert!(
            lines.concat()
                == format!(
                    "#\n#<date>\ncluster.id={CLUSTER_ID}\ndirectory.id={id}\nnode.id=2147483647\nversion=1\n"
                ),
            "{}",
            directory.display()
        );
    }
}
rstest_bdd_macros::scenarios!("tests/features/format_binary_parity", fixtures = [format_binary: FormatBinaryScenario]);

/// What one directory of a multi-directory format holds.
#[derive(Debug, PartialEq, Eq)]
struct Formatted {
    cluster_id: String,
    has_checkpoint: bool,
    has_manifest: bool,
    bootstrap_records: usize,
}

fn formatted(dir: &Path) -> Formatted {
    Formatted {
        cluster_id: written_cluster_id(dir),
        has_checkpoint: checkpoint(dir).is_file(),
        has_manifest: dir.join("bootstrap.json").is_file(),
        bootstrap_records: bootstrap::load_bootstrap_records(dir)
            .expect("bootstrap records")
            .len(),
    }
}

/// What a metadata directory holds after a format with `seed_records`
/// bootstrap records, and with the checkpoint of a dynamic format when
/// `has_checkpoint` is set.
fn metadata_directory(has_checkpoint: bool, seed_records: usize) -> Formatted {
    Formatted {
        cluster_id: CLUSTER_ID.to_owned(),
        has_checkpoint,
        has_manifest: true,
        bootstrap_records: seed_records,
    }
}

/// What a data directory holds: `meta.properties` and nothing else.
fn data_directory() -> Formatted {
    Formatted {
        cluster_id: CLUSTER_ID.to_owned(),
        has_checkpoint: false,
        has_manifest: false,
        bootstrap_records: 0,
    }
}

/// The number of bootstrap records a standalone format under [`CLUSTER_ID`]
/// seeds, read from a probe format in `root`.
fn standalone_seed_records(root: &Path) -> usize {
    let probe = root.join("probe");
    let mut args = vec!["--log-dir", path_str(&probe), "--cluster-id", CLUSTER_ID];
    args.extend_from_slice(STANDALONE);
    assert2::assert!(krabka_format(&args, None).status.success());
    bootstrap::load_bootstrap_records(&probe)
        .expect("records")
        .len()
}

/// Every file under `dir`, as a sorted list of `/`-separated relative paths.
fn files_under(dir: &Path) -> Vec<String> {
    snapshot(dir)
        .into_iter()
        .map(|(path, _)| {
            path.strip_prefix(dir)
                .expect("under dir")
                .components()
                .map(|part| part.as_os_str().to_str().expect("utf-8 path"))
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect()
}

/// Whether `line` is the date comment of `Properties.store` on a UTC host:
/// `#` and `java.util.Date.toString`, `EEE MMM dd HH:mm:ss zzz yyyy`.
fn is_utc_date_comment(line: &str) -> bool {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let digits = |field: &str, len: usize| {
        field.len() == len && field.bytes().all(|byte| byte.is_ascii_digit())
    };
    let Some(date) = line.strip_prefix('#') else {
        return false;
    };
    let fields: Vec<&str> = date.split(' ').collect();
    let [weekday, month, day, time, zone, year] = fields.as_slice() else {
        return false;
    };
    let clock: Vec<&str> = time.split(':').collect();
    WEEKDAYS.contains(weekday)
        && MONTHS.contains(month)
        && digits(day, 2)
        && clock.len() == 3
        && clock.iter().all(|field| digits(field, 2))
        && *zone == "UTC"
        && digits(year, 4)
}
