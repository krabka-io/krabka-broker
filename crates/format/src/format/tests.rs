//! Whole-format tests: the exit code each argv produces, and the files a run
//! leaves behind.
//!
//! These drive the command end to end through [`crate::run_from_args`], which
//! is the only way the mode selection, the voter-set validation, and the
//! writers become observable. The tests that pin one parser or one resolution
//! rule live beside that code in the sibling modules.

use {
    super::*,
    crate::{
        Cli, format::output::ZERO_CHECKPOINT_NAME, meta_properties::META_PROPERTIES, run_from_args,
    },
    assert2::check,
    std::{collections::HashSet, fs, path},
};

/// What a run leaves in one directory.
#[derive(Debug, PartialEq, Eq)]
struct DirectoryWant<Dir, Expected> {
    dir: Dir,
    expected: Expected,
}

/// A temporary root and the data directory path inside it.
struct DataDirectory {
    root: tempfile::TempDir,
    path: path::PathBuf,
}

fn fresh_data_directory() -> DataDirectory {
    let directory = tempfile::tempdir().expect("tempdir");
    let data = directory.path().join("data");
    DataDirectory {
        root: directory,
        path: data,
    }
}

fn format_argv(log_dir: &path::Path, node_id: &str, extra: &[&str]) -> Vec<String> {
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        node_id.to_owned(),
    ];
    argv.extend(extra.iter().map(|arg| (*arg).to_owned()));
    argv
}

/// The node id of every run below that does not test the node id.
const NODE_ID: &str = "1";

/// The exit code `run` returns for each argv it can be given.
///
/// Neither `run` nor `run_from_args` had a unit test, so a mutant making
/// either return a constant survived -- and with them the whole of
/// `is_dynamic_format` and `build_initial_voters`, whose only visible
/// effect is which of these codes comes back.
#[yare::parameterized(
    no_flags = { &[], EXIT_OK },
    standalone = { &["--standalone", "--controller-listener", "controller-1:9093"], EXIT_OK },
    no_initial_controllers = { &["--no-initial-controllers"], EXIT_OK },
    kraft_one_without_quorum = { &["--feature", "kraft.version=1"], EXIT_INVALID_FEATURE },
    standalone_kraft_zero = {
        &["--standalone", "--controller-listener", "c:9093", "--feature", "kraft.version=0"],
        EXIT_INVALID_FEATURE,
    },
    duplicate_kraft_feature = {
        &["--no-initial-controllers", "--feature", "kraft.version=1", "--feature", "kraft.version=1"],
        EXIT_INVALID_FEATURE,
    },
    kraft_above_range = { &["--feature", "kraft.version=2"], EXIT_INVALID_FEATURE },
    standalone_without_listener = { &["--standalone"], EXIT_BOOTSTRAP_FAIL },
    listener_without_port = {
        &["--standalone", "--controller-listener", "hostonly"],
        EXIT_BOOTSTRAP_FAIL,
    },
    listener_without_host = {
        &["--standalone", "--controller-listener", ":9093"],
        EXIT_BOOTSTRAP_FAIL,
    },
    listener_port_zero = { &["--standalone", "--controller-listener", "c:0"], EXIT_BOOTSTRAP_FAIL },
)]
#[test_macro(tokio::test)]
async fn exit_code_for_each_argv(extra: &[&str], want: i32) {
    let FreshArgv { dir: _tmp, argv } = fresh_argv(extra);
    let got = run_from_args(argv).await;
    assert2::check!(got == want, "exit {got}, want {want}");
}

struct WholeFormatScenario {
    root: tempfile::TempDir,
    metadata: Option<String>,
    log_dirs: Vec<String>,
    flags: Vec<String>,
    planned: Option<Result<Plan, Failure>>,
    exit: Option<i32>,
}
#[rstest::fixture]
fn whole_format() -> WholeFormatScenario {
    let root = tempfile::tempdir().expect("formatter root");
    WholeFormatScenario {
        root,
        metadata: None,
        log_dirs: vec!["a".into()],
        flags: Vec::new(),
        planned: None,
        exit: None,
    }
}
impl WholeFormatScenario {
    fn argv(&self) -> Vec<String> {
        let logs: Vec<&str> = self.log_dirs.iter().map(String::as_str).collect();
        let flags: Vec<&str> = self.flags.iter().map(String::as_str).collect();
        argv_under()
            .root(self.root.path())
            .maybe_metadata(self.metadata.as_deref())
            .log_dirs(&logs)
            .extra(&flags)
            .call()
    }
}
#[rstest_bdd_macros::given("format flags {flags}")]
fn format_validation_flags(whole_format: &mut WholeFormatScenario, flags: String) {
    whole_format.flags = flags.split_whitespace().map(str::to_owned).collect();
}
#[rstest_bdd_macros::given(
    "log directories {directories} with metadata directory {metadata} in {mode} mode"
)]
fn format_directory_selection(
    whole_format: &mut WholeFormatScenario,
    directories: String,
    metadata: String,
    mode: String,
) {
    whole_format.log_dirs = directories.split(',').map(str::to_owned).collect();
    whole_format.metadata = (metadata != "first-log-dir").then_some(metadata);
    whole_format.flags = match mode.as_str() {
        "static" => Vec::new(),
        "standalone" => STANDALONE.iter().map(|flag| (*flag).to_owned()).collect(),
        "nonvoter" => vec!["--no-initial-controllers".into()],
        _ => panic!("unknown format mode {mode}"),
    };
}
#[rstest_bdd_macros::given("directory {directory} contains unreadable meta.properties")]
fn unreadable_format_directory(whole_format: &mut WholeFormatScenario, directory: String) {
    let broken = whole_format.root.path().join(directory);
    fs::create_dir_all(&broken).expect("unreadable directory");
    fs::write(broken.join(META_PROPERTIES), UNREADABLE)
        .expect("unreadable meta.properties fixture");
}
#[rstest_bdd_macros::when("the formatter plans the run")]
fn plan_format_run(whole_format: &mut WholeFormatScenario) {
    whole_format.planned = Some(plan_of(whole_format.argv()));
}
#[rstest_bdd_macros::then(
    "the first formatter failure has Kafka feature exit code and begins {message}"
)]
fn first_formatter_error(whole_format: &WholeFormatScenario, message: String) {
    let Err(Failure {
        code,
        message: actual,
    }) = whole_format.planned.as_ref().expect("format planned")
    else {
        panic!("invalid format plan was accepted")
    };
    assert2::check!(*code == EXIT_INVALID_FEATURE, "exit {code}");
    assert2::check!(actual.starts_with(&message), "{actual}");
}
#[rstest_bdd_macros::then("the ordered format targets and directory kinds are")]
fn ordered_format_targets(whole_format: &WholeFormatScenario, datatable: Vec<Vec<String>>) {
    let plan = whole_format
        .planned
        .as_ref()
        .expect("format planned")
        .as_ref()
        .unwrap_or_else(|Failure { code, message }| panic!("exit {code}: {message}"));
    let got: Vec<DirectoryWant<PathBuf, DirectoryKind>> = plan
        .targets
        .iter()
        .map(|target| DirectoryWant {
            dir: target.dir.clone(),
            expected: target.kind,
        })
        .collect();
    let want: Vec<DirectoryWant<PathBuf, DirectoryKind>> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [directory, kind] = row.as_slice() else {
                panic!("directory and kind columns required")
            };
            let expected = match kind.as_str() {
                "data" => DirectoryKind::Data,
                "static-metadata" => DirectoryKind::StaticMetadata,
                "dynamic-metadata" => DirectoryKind::DynamicMetadata,
                "dynamic-voter" => DirectoryKind::DynamicMetadataVoter,
                _ => panic!("unknown directory kind {kind}"),
            };
            DirectoryWant {
                dir: whole_format.root.path().join(directory),
                expected,
            }
        })
        .collect();
    check!(got == want);
    let ids: HashSet<DirectoryId> = plan
        .targets
        .iter()
        .map(|target| target.directory_id)
        .collect();
    check!(ids.len() == plan.targets.len(), "{ids:?}");
}
#[rstest_bdd_macros::then("formatting refuses unreadable metadata directory {directory}")]
fn refused_metadata_directory(whole_format: &WholeFormatScenario, directory: String) {
    let expected = Failure {
        code: EXIT_DIRTY_LOG_DIR,
        message: format!(
            "Encountered I/O error in metadata log directory {}. Cannot continue.",
            whole_format.root.path().join(directory).display()
        ),
    };
    let actual = whole_format.planned.as_ref().expect("format planned");
    let Err(actual) = actual else {
        panic!("unreadable metadata directory was accepted")
    };
    check!(*actual == expected);
}
#[rstest_bdd_macros::then(
    "the formatter keeps target directories {targets} and reports unreadable directories {errors}"
)]
fn skipped_data_directory(whole_format: &WholeFormatScenario, targets: String, errors: String) {
    let plan = whole_format
        .planned
        .as_ref()
        .expect("format planned")
        .as_ref()
        .unwrap_or_else(|failure| panic!("unexpected refusal: {}", failure.message));
    let got_targets: Vec<PathBuf> = plan
        .targets
        .iter()
        .map(|target| target.dir.clone())
        .collect();
    let expected_targets: Vec<PathBuf> = targets
        .split(',')
        .map(|directory| whole_format.root.path().join(directory))
        .collect();
    let expected_errors: Vec<PathBuf> = errors
        .split(',')
        .map(|directory| whole_format.root.path().join(directory))
        .collect();
    check!(got_targets == expected_targets);
    check!(plan.errors == expected_errors);
}
#[rstest_bdd_macros::when("the formatter writes all selected directories")]
async fn run_whole_format(whole_format: &mut WholeFormatScenario) {
    whole_format.exit = Some(run_from_args(whole_format.argv()).await);
}
#[rstest_bdd_macros::then("the selected directories contain exactly these format files")]
fn whole_format_files(whole_format: &WholeFormatScenario, datatable: Vec<Vec<String>>) {
    check!(whole_format.exit == Some(EXIT_OK));
    let expected: Vec<DirectoryWant<String, Vec<String>>> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [directory, files] = row.as_slice() else {
                panic!("directory and files columns required")
            };
            DirectoryWant {
                dir: directory.clone(),
                expected: files.split(',').map(str::to_owned).collect(),
            }
        })
        .collect();
    let got: Vec<DirectoryWant<String, Vec<String>>> = expected
        .iter()
        .map(|wanted| DirectoryWant {
            dir: wanted.dir.clone(),
            expected: files_under(&whole_format.root.path().join(&wanted.dir)),
        })
        .collect();
    check!(got == expected);
    let directories: Vec<PathBuf> = expected
        .iter()
        .map(|wanted| whole_format.root.path().join(&wanted.dir))
        .collect();
    let ensemble = Ensemble::load(&directories).expect("survey formatted directories");
    check!(ensemble.formatted.len() == directories.len());
    check!(
        ensemble.verify(None, 1).is_ok_and(|id| id.is_some()),
        "one cluster id, node 1, and distinct directory ids"
    );
}

rstest_bdd_macros::scenarios!("tests/features/formatter_planning", fixtures = [whole_format: WholeFormatScenario]);

#[rstest_bdd_macros::scenario(
    path = "tests/features/formatter_bootstrap/formatter_bootstrap.feature",
    name = "Static bootstrap files belong only to a separate metadata directory"
)]
#[tokio::test]
async fn only_the_metadata_log_directory_gets_the_bootstrap_files(
    whole_format: WholeFormatScenario,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/formatter_bootstrap/formatter_bootstrap.feature",
    name = "Dynamic bootstrap and the zero checkpoint belong only to the metadata directory"
)]
#[tokio::test]
async fn separate_dynamic_metadata_gets_bootstrap_files(whole_format: WholeFormatScenario) {}
#[rstest_bdd_macros::scenario(
    path = "tests/features/formatter_bootstrap/formatter_bootstrap.feature",
    name = "The first log directory is the default dynamic metadata directory"
)]
#[tokio::test]
async fn default_dynamic_metadata_gets_bootstrap_files(whole_format: WholeFormatScenario) {}
#[rstest_bdd_macros::scenario(
    path = "tests/features/formatter_bootstrap/formatter_bootstrap.feature",
    name = "A metadata directory that is also a log directory is formatted once"
)]
#[tokio::test]
async fn shared_dynamic_metadata_gets_bootstrap_files(whole_format: WholeFormatScenario) {}

/// The exit code of a run, and the directory it formatted.
struct FormatRun {
    code: i32,
    log_dir: path::PathBuf,
}

/// Formatting into a fresh directory as node `node_id`, returning its path
/// for inspection.
async fn format_into(tmp: &path::Path, node_id: &str, extra: &[&str]) -> FormatRun {
    let log_dir = tmp.join("data");
    let argv = format_argv(&log_dir, node_id, extra);
    FormatRun {
        code: run_from_args(argv).await,
        log_dir,
    }
}

fn checkpoint_len(log_dir: &path::Path) -> u64 {
    let path = krabka_raft::metadata_partition_dir(log_dir).join(ZERO_CHECKPOINT_NAME);
    fs::metadata(path).map_or(0, |m| m.len())
}

/// Any one of the three quorum flags selects a dynamic format, and their
/// absence selects the static one. The offset-zero checkpoint is written
/// only for a dynamic format, so its presence is the observable.
#[yare::parameterized(
    static_format = { &[], false },
    standalone = { &["--standalone", "--controller-listener", "controller-1:9093"], true },
    explicit_controllers = {
        &["--initial-controllers", "1@host:9093:00000000-0000-0000-0000-000000000003"],
        true,
    },
    no_initial_controllers = { &["--no-initial-controllers"], true },
)]
#[test_macro(tokio::test)]
async fn each_quorum_flag_on_its_own_selects_a_dynamic_format(argv: &[&str], dynamic: bool) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let FormatRun { code, log_dir } = format_into(tmp.path(), NODE_ID, argv).await;
    assert2::check!(code == EXIT_OK, "exit {code}");
    assert2::check!(
        (checkpoint_len(&log_dir) > 0) == dynamic,
        "checkpoint present should be {dynamic}"
    );
}

/// The voter set rides in the checkpoint only when there is one. Both of
/// these formats are dynamic, so both write a checkpoint -- what separates
/// them is whether it also carries voters, and the one that does is bigger.
#[tokio::test]
async fn the_checkpoint_carries_voters_only_when_the_quorum_has_them() {
    let tmp_with = tempfile::tempdir().expect("tempdir");
    let FormatRun {
        code,
        log_dir: with_voters,
    } = format_into(tmp_with.path(), NODE_ID, STANDALONE).await;
    check!(code == EXIT_OK);

    let tmp_without = tempfile::tempdir().expect("tempdir");
    let FormatRun {
        code,
        log_dir: without_voters,
    } = format_into(tmp_without.path(), NODE_ID, &["--no-initial-controllers"]).await;
    check!(code == EXIT_OK);

    let (a, b) = (
        checkpoint_len(&with_voters),
        checkpoint_len(&without_voters),
    );
    check!(
        a > b,
        "checkpoint with voters ({a}) should exceed one without ({b})"
    );
}

/// The explicit quorum must name each controller once and must include
/// this node.
#[yare::parameterized(
    valid_pair = {
        "1",
        "1@host-a:9093:00000000-0000-0000-0000-000000000001,2@host-b:9093:00000000-0000-0000-0000-000000000002",
        EXIT_OK,
    },
    duplicate_node_id = {
        "1",
        "1@host-a:9093:00000000-0000-0000-0000-000000000001,1@host-c:9093:00000000-0000-0000-0000-00000000000c",
        EXIT_BOOTSTRAP_FAIL,
    },
    duplicate_directory_id = {
        "1",
        "1@host-a:9093:00000000-0000-0000-0000-000000000001,3@host-d:9093:00000000-0000-0000-0000-000000000001",
        EXIT_BOOTSTRAP_FAIL,
    },
    local_node_missing = {
        "9",
        "1@host-a:9093:00000000-0000-0000-0000-000000000001,2@host-b:9093:00000000-0000-0000-0000-000000000002",
        EXIT_BOOTSTRAP_FAIL,
    },
)]
#[test_macro(tokio::test)]
async fn an_explicit_quorum_is_checked_for_duplicates_and_for_this_node(
    node_id: &str,
    controllers: &str,
    want: i32,
) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let FormatRun { code, .. } =
        format_into(tmp.path(), node_id, &["--initial-controllers", controllers]).await;
    assert2::check!(code == want, "exit {code}, want {want}");
}

/// `--directory-id` is only checked against the quorum entry when it was
/// given, and only rejected when the two disagree.
#[yare::parameterized(
    matching = { "00000000-0000-0001-0000-000000000001", EXIT_OK },
    disagreement = { "00000000-0000-0000-0000-0000000000ff", EXIT_BOOTSTRAP_FAIL },
)]
#[test_macro(tokio::test)]
async fn an_explicit_directory_id_must_match_this_node_s_quorum_entry(
    directory_id: &str,
    want: i32,
) {
    // Outside the 100 lowest ids, which `--directory-id` refuses.
    const CONTROLLER: &str = "1@host-a:9093:00000000-0000-0001-0000-000000000001";
    let tmp = tempfile::tempdir().expect("tempdir");
    let FormatRun { code, .. } = format_into(tmp.path(), NODE_ID, &[
        "--initial-controllers",
        CONTROLLER,
        "--directory-id",
        directory_id,
    ])
    .await;
    assert2::check!(code == want, "exit {code}, want {want}");
}

/// The SCRAM iteration floor is inclusive: the minimum itself is allowed
/// and one below it is not.
#[yare::parameterized(
    minimum_allowed = { MIN_SCRAM_ITERATIONS as u32, EXIT_OK },
    one_below_refused = { MIN_SCRAM_ITERATIONS as u32 - 1, EXIT_LOW_ITERATIONS },
)]
#[test_macro(tokio::test)]
async fn scram_iterations_are_checked_against_an_inclusive_minimum(iterations: u32, want: i32) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let spec = format!("SCRAM-SHA-256=[name=alice,password=hunter2,iterations={iterations}]");
    let FormatRun { code, .. } = format_into(tmp.path(), NODE_ID, &["--add-scram", &spec]).await;
    assert2::check!(
        code == want,
        "iterations={iterations}: exit {code}, want {want}"
    );
}

/// A directory holding anything at all is refused rather than overwritten.
#[tokio::test]
async fn a_non_empty_log_dir_is_refused() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    fs::create_dir_all(&log_dir).expect("mkdir");
    fs::write(log_dir.join("someone-elses.txt"), b"x").expect("write");

    let code = run_from_args([
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--node-id",
        NODE_ID,
    ])
    .await;
    check!(code == EXIT_DIRTY_LOG_DIR);
}

/// The writers are only observable through the files they leave, so a
/// mutant emptying one out to `Ok(())` survives until something reads the
/// directory. A boot needs all of these.
#[tokio::test]
async fn a_standalone_format_writes_what_a_boot_reads() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();

    let code = run_from_args(standalone_argv(&log_dir, &[])).await;
    check!(code == EXIT_OK);

    for name in [META_PROPERTIES, "bootstrap.records.bin", "bootstrap.json"] {
        let path = log_dir.join(name);
        let len = fs::metadata(&path).map_or(0, |m| m.len());
        check!(len > 0, "{name} should exist and carry bytes, got {len}");
    }

    // KIP-853 dynamic quorum: the voter set lives in the offset-zero
    // checkpoint, not in the bootstrap record stream.
    let len = checkpoint_len(&log_dir);
    check!(len > 0, "offset-zero checkpoint should carry the voter set");
}

fn standalone_argv(log_dir: &path::Path, extra: &[&str]) -> Vec<String> {
    let mut argv = [
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "controller-1:9093",
    ]
    .map(str::to_owned)
    .to_vec();
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    argv
}

/// `--ignore-formatted` is what lets a Kubernetes init container run the
/// formatter unconditionally: the second run is a no-op that exits 0 and
/// leaves the first run's identity in place, while the same directory without
/// the flag is still refused.
#[tokio::test]
async fn ignore_formatted_makes_a_second_format_a_no_op() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    let argv = |extra: &[&str]| standalone_argv(&log_dir, extra);

    check!(crate::run_from_args(argv(&[])).await == EXIT_OK);
    let formatted = fs::read(log_dir.join(META_PROPERTIES)).expect("meta properties");

    // Without the flag the same directory is still a dirty log dir.
    check!(crate::run_from_args(argv(&[])).await == EXIT_DIRTY_LOG_DIR);

    // With it the run succeeds and rewrites nothing: a regenerated cluster or
    // directory id would strand the node's replicated identity.
    check!(crate::run_from_args(argv(&["--ignore-formatted"])).await == EXIT_OK);
    let after = fs::read(log_dir.join(META_PROPERTIES)).expect("meta properties");
    check!(after == formatted);
}

/// An unformatted directory is formatted normally under the flag: it means
/// "ignore an existing format", not "skip formatting".
#[tokio::test]
async fn ignore_formatted_still_formats_a_fresh_directory() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();

    let code = run_from_args(standalone_argv(&log_dir, &["--ignore-formatted"])).await;
    check!(code == EXIT_OK);
    check!(log_dir.join(META_PROPERTIES).is_file());
}

/// A format that fails partway leaves no marker, so the next run redoes it
/// rather than treating the half-written directory as formatted.
///
/// `--ignore-formatted` is what makes the formatter safe to run
/// unconditionally, and the price of that is that whatever it recognises as
/// "already formatted" has to mean the whole format landed. A rejected
/// `--add-scram` fails after the point the identity is resolved and before
/// any output is written, which is exactly the window that would otherwise
/// strand a directory carrying an identity and no seed records: the next
/// init-container attempt would exit 0 on it and the broker would boot with
/// no offset-zero checkpoint and no voter set.
#[tokio::test]
async fn a_failed_format_is_not_mistaken_for_a_finished_one() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    let argv = |extra: &[&str]| standalone_argv(&log_dir, extra);

    let weak = "SCRAM-SHA-256=[name=alice,password=hunter2,iterations=1]";
    check!(crate::run_from_args(argv(&["--add-scram", weak])).await == EXIT_LOW_ITERATIONS);
    check!(!log_dir.join(META_PROPERTIES).exists());

    // The retry an init container makes formats the directory for real.
    check!(crate::run_from_args(argv(&["--ignore-formatted"])).await == EXIT_OK);
    check!(log_dir.join(META_PROPERTIES).is_file());
    check!(log_dir.join("bootstrap.json").is_file());
    check!(log_dir.join("bootstrap.records.bin").is_file());
    check!(checkpoint_len(&log_dir) > 0, "the voter set must be seeded");
}

/// A directory whose `meta.properties` does not read is skipped with Kafka's
/// message and the others are formatted, unless it is the metadata log
/// directory, which Kafka refuses to continue without.
#[tokio::test]
async fn an_unreadable_marker_is_skipped_unless_it_is_the_metadata_directory() {
    let (tmp, meta_dir, data_dir) = metadata_and_data_directories();
    let unreadable = |dir: &path::Path| {
        fs::create_dir_all(dir).expect("mkdir");
        fs::write(dir.join(META_PROPERTIES), UNREADABLE).expect("write");
    };
    let run = |first: &path::Path, second: &path::Path| {
        run_from_args([
            "krabka-format".to_owned(),
            "--log-dir".to_owned(),
            first.display().to_string(),
            "--log-dir".to_owned(),
            second.display().to_string(),
            "--node-id".to_owned(),
            NODE_ID.to_owned(),
        ])
    };

    unreadable(&data_dir);
    check!(run(&meta_dir, &data_dir).await == EXIT_OK);
    check!(meta_dir.join(META_PROPERTIES).is_file());
    check!(fs::read(data_dir.join(META_PROPERTIES)).expect("left alone") == UNREADABLE);

    let other = tmp.path().join("other");
    check!(run(&data_dir, &other).await == EXIT_DIRTY_LOG_DIR);
    check!(!other.exists());
}

/// The quorum flags of a standalone format.
const STANDALONE: &[&str] = &["--standalone", "--controller-listener", "c:9093"];

/// A `meta.properties` that does not read: its version is not a number.
const UNREADABLE: &[u8] = b"version=one\n";

/// The argv of a run under `root`: `--metadata-log-dir` when `metadata` names
/// one, a `--log-dir` for each of `log_dirs`, `--node-id 1`, and then `extra`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FormatArgvSetup<'a> {
    metadata: Option<&'a str>,
    #[default(&["a"])]
    log_dirs: &'a [&'a str],
    extra: &'a [&'a str],
}

#[bon::builder]
fn argv_under(
    root: &Path,
    metadata: Option<&'_ str>,
    #[builder(default = & ["a"])] log_dirs: &'_ [&'_ str],
    #[builder(default)] extra: &'_ [&'_ str],
) -> Vec<String> {
    let setup = FormatArgvSetup {
        metadata,
        log_dirs,
        extra,
    };
    let FormatArgvSetup {
        metadata,
        log_dirs,
        extra,
    } = setup;
    let mut argv = vec!["krabka-format".to_owned()];
    if let Some(dir) = metadata {
        argv.push("--metadata-log-dir".to_owned());
        argv.push(root.join(dir).display().to_string());
    }
    for dir in log_dirs {
        argv.push("--log-dir".to_owned());
        argv.push(root.join(dir).display().to_string());
    }
    argv.push("--node-id".to_owned());
    argv.push(NODE_ID.to_owned());
    argv.extend(extra.iter().map(|a| (*a).to_owned()));
    argv
}

/// The plan for `argv`.
fn plan_of(argv: Vec<String>) -> Result<Plan, Failure> {
    use clap::Parser as _;

    plan(Cli::try_parse_from(argv).expect("parse").args, Vec::new())
}

/// `--directory-id` and the local `--initial-controllers` entry name the id of
/// the metadata log directory, also when `--metadata-log-dir` puts it apart
/// from the log directories. A data directory gets another id.
#[yare::parameterized(
    static_directory_id = {
        &["--directory-id", "00000000-0000-0000-0000-000000000064"],
        DirectoryKind::StaticMetadata,
    },
    dynamic_voter_directory_id = {
        &["--initial-controllers", "1@controller-1:9093:00000000-0000-0000-0000-000000000064"],
        DirectoryKind::DynamicMetadataVoter,
    },
)]
fn the_directory_id_belongs_to_the_metadata_log_directory(extra: &[&str], kind: DirectoryKind) {
    let id = DirectoryId(uuid::Uuid::from_u128(0x64));
    let tmp = tempfile::tempdir().expect("tempdir");
    let plan = plan_of(
        argv_under()
            .root(tmp.path())
            .maybe_metadata(Some("m"))
            .extra(extra)
            .call(),
    )
    .unwrap_or_else(|Failure { code, message }| panic!("exit {code}: {message}"));

    let [metadata, data] = plan.targets.as_slice() else {
        panic!("{:?}", plan.targets);
    };
    assert2::check!(
        *metadata
            == Target {
                dir: tmp.path().join("m"),
                kind,
                directory_id: id,
            }
    );
    assert2::check!(
        *data
            == Target {
                dir: tmp.path().join("a"),
                kind: DirectoryKind::Data,
                directory_id: data.directory_id,
            }
    );
    assert2::check!(data.directory_id != id);
}

krabka_macros::directory_tree_fixture!(
    files_under,
    String,
    { /// Every file under `dir`, as a sorted list of `/`-separated relative paths.
    },
    (|dir: &Path, path: &Path| {
                let relative = path.strip_prefix(dir).expect("under dir");
                let parts: Vec<String> = relative
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy().into_owned())
                    .collect();
                parts.join("/")
    })
);

/// A formatted directory of another node refuses the run, `--ignore-formatted`
/// or not, with Kafka's message: `Formatter.doFormat` verifies the set against
/// `node.id` before it looks at what is already formatted.
#[yare::parameterized(
    no_ignore_flag = { &[] },
    ignore_formatted = { &["--ignore-formatted"] },
)]
#[test_macro(tokio::test)]
async fn a_directory_of_another_node_is_refused(extra: &[&str]) {
    let tmp = tempfile::tempdir().expect("tempdir");
    check!(crate::run_from_args(argv_under().root(tmp.path()).call()).await == EXIT_OK);
    let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
    let message = format!(
        "Stored node id 1 doesn't match previous node id 2 in {}. If you moved your data, make \
         sure your configured node id matches. If you intend to create a new node, you should \
         remove all data in your data directories.",
        a.join(META_PROPERTIES).display()
    );

    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        format!("{},{}", a.display(), b.display()),
        "--node-id".to_owned(),
        "2".to_owned(),
    ];
    argv.extend(extra.iter().map(|flag| (*flag).to_owned()));
    assert2::check!(
        plan_of(argv).err()
            == Some(Failure {
                code: EXIT_DIRTY_LOG_DIR,
                message,
            }),
        "{extra:?}"
    );
    assert2::check!(!b.exists(), "a refused run writes nothing");
}

/// A temporary root and the argv that formats a fresh directory under it.
struct FreshArgv {
    dir: tempfile::TempDir,
    argv: Vec<String>,
}

fn fresh_argv(extra: &[&str]) -> FreshArgv {
    let DataDirectory {
        root: dir,
        path: log_dir,
    } = fresh_data_directory();
    let argv = format_argv(&log_dir, NODE_ID, extra);
    FreshArgv { dir, argv }
}

krabka_macros::format_directories_fixture!(metadata_and_data_directories);
