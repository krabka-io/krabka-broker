//! The `kafka-consumer-groups` administration tool: `--list`, `--describe`, and
//! the KIP-496 `--delete-offsets` path.
//!
//! These runs exercise the JVM `AdminClient` group APIs rather than a consumer,
//! so they stay apart from the `kafka-console-consumer` suites.

use {
    crate::{
        jvm_acceptance::{
            KAFKA_IMAGE, broker0_advertised, describe_console_group, docker_run_kafka_tool,
            start_console_group,
        },
        oracle::OracleComparison,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{
        io::Write as _,
        process::Output,
        thread,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

struct ConsoleAdministration {
    broker: Option<(BrokerHandle, TempDir)>,
    topic: String,
    group: String,
    output: Option<Output>,
    description: Option<Output>,
}
#[rstest::fixture]
fn console() -> ConsoleAdministration {
    // Start this scenario with fresh inputs and observations.

    ConsoleAdministration {
        broker: None,
        topic: String::new(),
        group: String::new(),
        output: None,
        description: None,
    }
}
#[rstest_bdd_macros::given(
    "console group {group} consumes alpha from topic {topic} with {partitions} partitions and closes"
)]
async fn console_group(
    console: &mut ConsoleAdministration,
    group: String,
    topic: String,
    partitions: i32,
) {
    console.broker = Some(start_console_group(&topic, &group, partitions).await);
    console.topic = topic;
    console.group = group;
}
#[rstest_bdd_macros::when("the group tool lists groups and describes the console group")]
fn group_listing(console: &mut ConsoleAdministration) {
    console.output = Some(docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]));
    console.description = Some(describe_console_group(&console.group));
}
#[rstest_bdd_macros::then("the listing contains the group and its description contains the topic")]
fn group_visible(console: &ConsoleAdministration) {
    assert!(
        String::from_utf8_lossy(&console.output.as_ref().expect("listing").stdout)
            .contains(&console.group)
    );
    assert!(
        String::from_utf8_lossy(&console.description.as_ref().expect("description").stdout)
            .contains(&console.topic)
    );
}
#[rstest_bdd_macros::when("the administrator describes the console group's committed offsets")]
fn describe_commits(console: &mut ConsoleAdministration) {
    console.description = Some(describe_console_group(&console.group));
}
#[rstest_bdd_macros::then("the group's committed offset description contains its topic")]
fn committed_topic(console: &ConsoleAdministration) {
    assert!(
        String::from_utf8_lossy(&console.description.as_ref().expect("description").stdout)
            .contains(&console.topic)
    );
}
#[rstest_bdd_macros::when(
    "the Kafka 2.7 tool deletes that group's topic offsets with a piped yes confirmation"
)]
fn delete_offsets(console: &mut ConsoleAdministration) {
    let mut child = jvm_spawn_piped(
        &mut jvm_docker_command()
            .image(KAFKA_IMAGE)
            .args(&[
                "kafka-consumer-groups",
                "--bootstrap-server",
                broker0_advertised(),
                "--delete-offsets",
                "--group",
                &console.group,
                "--topic",
                &console.topic,
            ])
            .input(ContainerInput::Attached)
            .call(),
        "spawn delete-offsets",
    );
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "y").expect("write y");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait delete-offsets");
    console.output = Some(out);
    console.description = Some(describe_console_group(&console.group));
}
#[rstest_bdd_macros::then(
    "offset deletion succeeds, identifies success or the topic, and no group-topic row remains"
)]
fn offsets_removed(console: &ConsoleAdministration) {
    let output = console.output.as_ref().expect("delete attempted");
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("Successful") || text.contains(&console.topic));
    let described = String::from_utf8_lossy(
        &console
            .description
            .as_ref()
            .expect("post delete description")
            .stdout,
    );
    assert!(
        !described
            .lines()
            .any(|line| line.starts_with(&console.group) && line.contains(&console.topic))
    );
}
async fn shutdown_console(console: &mut ConsoleAdministration) {
    console.broker.take().expect("broker").0.shutdown().await;
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/consumer_groups.feature",
    name = "kafka consumer groups list describe"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_consumer_groups_list_describe(console: ConsoleAdministration) {
    shutdown_console(&mut console).await;
    drop(console);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/consumer_groups.feature",
    name = "kafka consumer groups delete offsets"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_consumer_groups_delete_offsets(console: ConsoleAdministration) {
    shutdown_console(&mut console).await;
    drop(console);
}

// ── `--reset-offsets` and `--delete`, against the stock broker ──────────────
//
// Everything below runs the *same* `kafka-consumer-groups` binary, out of the
// same `apache/kafka:4.3.1` image, against krabka and against a stock broker of
// that release, and compares what the tool printed once it has been parsed.
// The two clusters are put in the same shape first, by the same commands, so a
// difference in the answer is a difference in the broker.
//
// The oracle is asked first in every case. If the rule a case states is wrong
// about Kafka, that is where the suite says so, before krabka is blamed for
// missing it.

use crate::{
    group_output::{
        DeleteOutcome, ResetRow, kafka_exceptions, parse_delete_groups, parse_export_csv,
        parse_reset_table,
    },
    oracle::{CliRun, Side, ToolFile},
    support::{ContainerInput, jvm_docker_command, jvm_spawn_piped},
};

/// The topic every reset case reads. One partition, so the record-to-partition
/// mapping is not the producer's to choose: Kafka's default partitioner is
/// sticky, and with two partitions the same six records land differently on
/// the two sides and every expected offset below becomes a coin toss.
const RESET_TOPIC: &str = "krabka-reset-offsets-itest";

/// How many records the topic holds, and therefore its log end offset.
const RESET_RECORDS: i64 = 6;

/// Where the seeded group's committed offset is parked.
///
/// Strictly between `0` and [`RESET_RECORDS`], which is what makes
/// `--to-current` a distinguishable answer: parked at either end it would be
/// indistinguishable from `--to-earliest` or `--to-latest`, and a broker that
/// confused the three would pass.
const RESET_CURRENT: i64 = 3;

/// Where the `--from-file` and `--execute` cases put their offsets document
/// inside whichever container the tool runs in.
const OFFSETS_CSV: &str = "/tmp/krabka-offsets.csv";

/// One `kafka-consumer-groups` invocation against `side`.
fn consumer_groups(side: &Side<'_>, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    side.run("kafka-consumer-groups", &full)
}

/// One `kafka-consumer-groups` invocation that reads a file the harness wrote.
fn consumer_groups_with_file(side: &Side<'_>, csv: &str, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    side.run_with_files(
        "kafka-consumer-groups",
        &full,
        &[ToolFile::new(OFFSETS_CSV, csv)],
        None,
    )
}

/// Create [`RESET_TOPIC`] on `side` and fill it with [`RESET_RECORDS`]
/// records.
fn seed_topic(side: &Side<'_>) {
    side.run("kafka-topics", &[
        "--bootstrap-server",
        side.bootstrap(),
        "--create",
        "--if-not-exists",
        "--topic",
        RESET_TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ])
    .expect_success();

    let mut records = String::new();
    for index in 0..RESET_RECORDS {
        use std::fmt::Write as _;
        writeln!(records, "record-{index}").expect("a String never fails to grow");
    }
    side.run_with_files(
        "kafka-console-producer",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--topic",
            RESET_TOPIC,
        ],
        &[],
        Some(&records),
    )
    .expect_success();
}

/// Register `group` on `side` with its committed offset at
/// [`RESET_CURRENT`].
///
/// The consumer reads the whole topic and exits, which both commits an offset
/// and leaves the group `Empty` -- the state `--reset-offsets` requires. The
/// reset that follows moves the commit off the end of the log, so the three
/// interesting positions are three different numbers.
fn seed_group(side: &Side<'_>, group: &str) {
    side.run("kafka-console-consumer", &[
        "--bootstrap-server",
        side.bootstrap(),
        "--topic",
        RESET_TOPIC,
        "--group",
        group,
        "--from-beginning",
        "--max-messages",
        &RESET_RECORDS.to_string(),
        "--timeout-ms",
        "30000",
    ])
    .expect_success();

    consumer_groups(side, &[
        "--group",
        group,
        "--topic",
        RESET_TOPIC,
        "--reset-offsets",
        "--to-offset",
        &RESET_CURRENT.to_string(),
        "--execute",
    ])
    .expect_success();
}

/// The committed offset `--describe` reports for `group` on partition 0.
fn committed_offset(side: &Side<'_>, group: &str) -> Option<i64> {
    let described = consumer_groups(side, &["--describe", "--group", group]).expect_success();
    described
        .stdout
        .lines()
        .filter(|line| line.contains(group) && line.contains(RESET_TOPIC))
        .find_map(|line| {
            // `GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG …`
            let fields: Vec<&str> = line.split_whitespace().collect();
            fields.get(3)?.parse().ok()
        })
}

/// The rows a mode must produce, given the group it was run for.
fn expected_rows(group: &str, new_offset: i64) -> Vec<ResetRow> {
    vec![ResetRow {
        group: group.to_owned(),
        topic: RESET_TOPIC.to_owned(),
        partition: 0,
        new_offset,
    }]
}

struct OffsetAdministration {
    comparison: Option<OracleComparison>,
    dry_group: String,
    execute_group: String,
    file_group: String,
    dry: Vec<(CliRun, Option<i64>)>,
    executed: Vec<(CliRun, Option<i64>)>,
    exports: Vec<CliRun>,
    restored: Vec<(CliRun, CliRun, Option<i64>)>,
}
#[rstest::fixture]
fn resets() -> OffsetAdministration {
    // Start this scenario with fresh inputs and observations.

    OffsetAdministration {
        comparison: None,
        dry_group: String::new(),
        execute_group: String::new(),
        file_group: String::new(),
        dry: Vec::new(),
        executed: Vec::new(),
        exports: Vec::new(),
        restored: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka and Krabka have six records on krabka-reset-offsets-itest and these groups parked at offset 3"
)]
async fn reset_groups(resets: &mut OffsetAdministration, datatable: Vec<Vec<String>>) {
    resets.comparison = Some(OracleComparison::start("reset-offsets").await);
    let groups: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [group]: [String; 1] = row.try_into().expect("group");
            group
        })
        .collect();
    let [dry, execute, file]: [String; 3] = groups.try_into().expect("three groups");
    for side in resets.comparison.as_ref().expect("comparison").sides() {
        seed_topic(&side);
        for group in [&dry, &execute, &file] {
            seed_group(&side, group);
        }
    }
    resets.dry_group = dry;
    resets.execute_group = execute;
    resets.file_group = file;
}
#[rstest_bdd_macros::when("both group tools dry-run these reset arguments in order")]
fn dry_modes(resets: &mut OffsetAdministration, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [mode]: [String; 1] = row.try_into().expect("mode arguments");
        for side in resets.comparison.as_ref().expect("comparison").sides() {
            let mut args = vec![
                "--group",
                &resets.dry_group,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
            ];
            args.extend(mode.split_whitespace());
            args.push("--dry-run");
            let output = consumer_groups(&side, &args);
            let commit = committed_offset(&side, &resets.dry_group);
            resets.dry.push((output, commit));
        }
    }
}
#[rstest_bdd_macros::then(
    "both tools propose these offsets in order and every dry run leaves committed offset 3 unchanged"
)]
fn dry_results(resets: &OffsetAdministration, datatable: Vec<Vec<String>>) {
    let expected: Vec<i64> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [offset]: [String; 1] = row.try_into().expect("offset");
            offset.parse().expect("offset number")
        })
        .collect();
    assert!(resets.dry.len() == expected.len() * 2);
    for (pair, offset) in resets.dry.as_chunks::<2>().0.iter().zip(expected) {
        for (output, commit) in pair {
            assert!(output.succeeded(), "{}", output.text());
            assert!(parse_reset_table(&output.stdout) == expected_rows(&resets.dry_group, offset));
            assert!(*commit == Some(3));
        }
        assert!(parse_reset_table(&pair[0].0.stdout) == parse_reset_table(&pair[1].0.stdout));
    }
}
#[rstest_bdd_macros::when("both tools execute to-earliest for the execute group")]
fn execute_earliest(resets: &mut OffsetAdministration) {
    for side in resets.comparison.as_ref().expect("comparison").sides() {
        let output = consumer_groups(&side, &[
            "--group",
            &resets.execute_group,
            "--topic",
            RESET_TOPIC,
            "--reset-offsets",
            "--to-earliest",
            "--execute",
        ]);
        let commit = committed_offset(&side, &resets.execute_group);
        resets.executed.push((output, commit));
    }
}
#[rstest_bdd_macros::then("both execution tables and committed offsets become 0")]
fn execute_results(resets: &OffsetAdministration) {
    assert!(resets.executed.len() == 2);
    for (output, commit) in &resets.executed {
        assert!(output.succeeded(), "{}", output.text());
        assert!(parse_reset_table(&output.stdout) == expected_rows(&resets.execute_group, 0));
        assert!(*commit == Some(0));
    }
}
#[rstest_bdd_macros::when("both tools export the file group's current offsets")]
fn export_offsets(resets: &mut OffsetAdministration) {
    for side in resets.comparison.as_ref().expect("comparison").sides() {
        resets.exports.push(consumer_groups(&side, &[
            "--group",
            &resets.file_group,
            "--topic",
            RESET_TOPIC,
            "--reset-offsets",
            "--to-current",
            "--export",
        ]));
    }
}
#[rstest_bdd_macros::then("both exports contain one matching topic row at offset 3")]
fn export_results(resets: &OffsetAdministration) {
    assert!(resets.exports.len() == 2);
    for output in &resets.exports {
        assert!(output.succeeded(), "{}", output.text());
        let rows = parse_export_csv(&output.stdout);
        assert!(rows.len() == 1 && rows[0].topic == RESET_TOPIC && rows[0].offset == 3);
    }
    assert!(
        parse_export_csv(&resets.exports[0].stdout) == parse_export_csv(&resets.exports[1].stdout)
    );
}
#[rstest_bdd_macros::when("both file groups move to earliest and execute from their exported CSV")]
fn restore_offsets(resets: &mut OffsetAdministration) {
    for (side, exported) in resets
        .comparison
        .as_ref()
        .expect("comparison")
        .sides()
        .into_iter()
        .zip(&resets.exports)
    {
        let moved = consumer_groups(&side, &[
            "--group",
            &resets.file_group,
            "--topic",
            RESET_TOPIC,
            "--reset-offsets",
            "--to-earliest",
            "--execute",
        ]);
        let restored = consumer_groups_with_file(&side, &exported.stdout, &[
            "--group",
            &resets.file_group,
            "--reset-offsets",
            "--from-file",
            OFFSETS_CSV,
            "--execute",
        ]);
        let commit = committed_offset(&side, &resets.file_group);
        resets.restored.push((moved, restored, commit));
    }
}
#[rstest_bdd_macros::then("both from-file result tables and committed offsets return to 3")]
fn restore_results(resets: &OffsetAdministration) {
    assert!(resets.restored.len() == 2);
    for (moved, restored, commit) in &resets.restored {
        assert!(moved.succeeded(), "{}", moved.text());
        assert!(restored.succeeded(), "{}", restored.text());
        assert!(parse_reset_table(&restored.stdout) == expected_rows(&resets.file_group, 3));
        assert!(*commit == Some(3));
    }
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/consumer_groups.feature",
    name = "reset offsets modes match apache kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_offsets_modes_match_apache_kafka(resets: OffsetAdministration) {
    resets
        .comparison
        .take()
        .expect("comparison")
        .broker
        .shutdown()
        .await;
    drop(resets);
}

struct GroupLifecycle {
    comparison: Option<OracleComparison>,
    live: String,
    empty: String,
    absent: String,
    live_runs: Vec<(CliRun, CliRun)>,
    final_runs: Vec<(CliRun, CliRun, CliRun)>,
}
#[rstest::fixture]
fn lifecycle() -> GroupLifecycle {
    // Start this scenario with fresh inputs and observations.

    GroupLifecycle {
        comparison: None,
        live: String::new(),
        empty: String::new(),
        absent: String::new(),
        live_runs: Vec::new(),
        final_runs: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka and Krabka have six records, empty group {empty}, absent group {absent}, and a future live group {live}"
)]
async fn lifecycle_groups(
    lifecycle: &mut GroupLifecycle,
    empty: String,
    absent: String,
    live: String,
) {
    lifecycle.comparison = Some(OracleComparison::start("group-lifecycle").await);
    for side in lifecycle.comparison.as_ref().expect("comparison").sides() {
        seed_topic(&side);
        seed_group(&side, &empty);
    }
    lifecycle.empty = empty;
    lifecycle.absent = absent;
    lifecycle.live = live;
}
#[rstest_bdd_macros::when(
    "a detached consumer joins each live group and both tools try to reset then delete it"
)]
fn reject_live(lifecycle: &mut GroupLifecycle) {
    for side in lifecycle.comparison.as_ref().expect("comparison").sides() {
        let consumer = side.run_detached("kafka-console-consumer", &[
            "--bootstrap-server",
            side.bootstrap(),
            "--topic",
            RESET_TOPIC,
            "--group",
            &lifecycle.live,
            "--from-beginning",
        ]);
        wait_until_group_is_live(&side, &lifecycle.live);
        let reset = consumer_groups(&side, &[
            "--group",
            &lifecycle.live,
            "--topic",
            RESET_TOPIC,
            "--reset-offsets",
            "--to-earliest",
            "--dry-run",
        ]);
        let delete = consumer_groups(&side, &["--delete", "--group", &lifecycle.live]);
        lifecycle.live_runs.push((reset, delete));
        drop(consumer);
    }
}
#[rstest_bdd_macros::then(
    "live resets propose no offsets and explain can only be reset if the group while deletions report {exception}"
)]
fn live_refusals(lifecycle: &GroupLifecycle, exception: String) {
    assert!(lifecycle.live_runs.len() == 2);
    for (reset, delete) in &lifecycle.live_runs {
        assert!(reset.text().contains("can only be reset if the group"));
        assert!(parse_reset_table(&reset.text()).is_empty());
        assert!(
            parse_delete_groups(&delete.text())
                == vec![DeleteOutcome {
                    group: lifecycle.live.clone(),
                    failure: Some(exception.clone())
                }]
        );
    }
}
#[rstest_bdd_macros::when(
    "both tools delete the absent and empty groups then list remaining groups"
)]
fn delete_remaining(lifecycle: &mut GroupLifecycle) {
    for side in lifecycle.comparison.as_ref().expect("comparison").sides() {
        lifecycle.final_runs.push((
            consumer_groups(&side, &["--delete", "--group", &lifecycle.absent]),
            consumer_groups(&side, &["--delete", "--group", &lifecycle.empty]),
            consumer_groups(&side, &["--list"]),
        ));
    }
}
#[rstest_bdd_macros::then(
    "absent groups report {exception} while empty groups delete successfully and disappear from successful listings"
)]
fn lifecycle_results(lifecycle: &GroupLifecycle, exception: String) {
    assert!(lifecycle.final_runs.len() == 2);
    for (absent, empty, listed) in &lifecycle.final_runs {
        assert!(
            parse_delete_groups(&absent.text())
                == vec![DeleteOutcome {
                    group: lifecycle.absent.clone(),
                    failure: Some(exception.clone())
                }]
        );
        assert!(empty.succeeded() && kafka_exceptions(&empty.text()).is_empty());
        assert!(listed.succeeded());
        assert!(!listed.stdout.contains(&lifecycle.empty));
    }
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/consumer_groups.feature",
    name = "group delete and reset refusals match apache kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_delete_and_reset_refusals_match_apache_kafka(lifecycle: GroupLifecycle) {
    lifecycle
        .comparison
        .take()
        .expect("comparison")
        .broker
        .shutdown()
        .await;
    drop(lifecycle);
}

/// Poll `--describe --state` until `group` reports `Stable`.
///
/// A `kafka-console-consumer` container takes a few seconds to start a JVM and
/// finish a join, and every assertion after it is about a group that is not
/// `Empty`. Without this wait the refusals would race the consumer and the
/// case would sometimes assert that Kafka refuses a reset it in fact allows.
///
/// `Stable` and not merely "not `Empty`": a group nobody has ever joined is
/// absent rather than empty, and the tool answers for it with a sentence that
/// names the group and no state at all -- which a negative test would read as
/// success.
fn wait_until_group_is_live(side: &Side<'_>, group: &str) {
    const BUDGET: Duration = Duration::from_secs(120);
    let deadline = Instant::now() + BUDGET;
    loop {
        let state = consumer_groups(side, &["--describe", "--state", "--group", group]);
        if state.succeeded() && state.stdout.contains(group) && state.stdout.contains("Stable") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{}: {group} never reached Stable within {BUDGET:?}:\n{}",
            side.label(),
            state.text(),
        );
        thread::sleep(Duration::from_secs(2));
    }
}
