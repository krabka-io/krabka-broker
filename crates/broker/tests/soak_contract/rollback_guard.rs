//! The rollback a lifecycle contract may not take, and the proof that the
//! source release refuses it before it damages anything.
//!
//! The rule in `docs/persisted_formats.md` lets a cluster roll back only while
//! every finalized feature level is one the source supports. Here the
//! candidate formats one node at `4.4-IV2`, a `metadata.version` above the
//! source's range that only `unstable.feature.versions.enable` reaches, runs
//! it with that switch on, and writes a record. The node is then started on
//! the source release with the switch off, as an operator's rollback would
//! start it.
//!
//! The source must exit non-zero with Kafka's message for the level it does
//! not support, and leave every byte of the log directory as it was, apart
//! from what a broker rebuilds from the log as it opens it: producer-state
//! snapshots (`*.snapshot`) and the offset and time indexes of a segment
//! (`*.index`, `*.timeindex`), which Kafka too deletes and rebuilds from the
//! segment when it finds them wanting. Every segment, checkpoint,
//! `quorum-state` and other file must be byte for byte the same. The
//! candidate must then open the same directory again and read the record
//! back.

use {
    crate::{
        cluster::{SoakBroker, SoakCluster, create_topics},
        host::docker_try,
        lifecycle_run::source_image,
    },
    krabka_bench_driver::soak::{
        client::{
            Endpoint,
            load::{producer_name, record_value},
            readback::read_back,
        },
        contract::{Contract, Isolation, LoadPath},
        lifecycle::LevelRange,
    },
    krabka_client_producer::{Acks, Producer, ProducerRecord},
    krabka_protocol::owned::create_topics_request::CreatableTopic,
    std::{
        collections::BTreeMap,
        fs,
        path::Path,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// The `metadata.version` the candidate finalizes: Kafka trunk's `4.4-IV2`.
const UNSTABLE_RELEASE: &str = "4.4-IV2";

/// Its level.
const UNSTABLE_LEVEL: i16 = 33;

/// The candidate's node configuration addition that supports it.
const UNSTABLE_TOML: &str =
    "\n[server_properties]\n\"unstable.feature.versions.enable\" = \"true\"\n";

/// How long the source gets to refuse. It refuses as it opens the log.
const REFUSAL_LIMIT: Duration = Duration::from_secs(90);

/// The topic the record is written to.
const TOPIC: &str = "rollback-guard";

/// What the refused rollback left, for the evidence.
#[derive(Debug, serde::Serialize)]
pub struct RefusedRollback {
    pub(crate) source_exit_code: i64,
    pub(crate) refusal: String,
    /// Files whose bytes changed, appeared or disappeared, other than what
    /// a broker derives as it opens the log.
    pub(crate) changed: Vec<String>,
    /// Producer-state snapshots and segment indexes that changed, appeared
    /// or disappeared.
    pub(crate) derived_changed: Vec<String>,
    pub(crate) record_read_back: bool,
}

/// The bytes of every file under `dir`, by path relative to it. The guard's
/// node holds one record, so its directory is small.
pub fn file_contents(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut contents = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(bytes) = fs::read(&path) {
                let relative = path.strip_prefix(dir).map_or_else(
                    |_| path.display().to_string(),
                    |relative| relative.display().to_string(),
                );
                contents.insert(relative, bytes);
            }
        }
    }
    contents
}

/// Whether `name` is a file a broker rebuilds from the log as it opens it: a
/// producer-state snapshot, or a segment's offset or time index.
fn derived(name: &str) -> bool {
    let extension = Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default();
    matches!(extension, "snapshot" | "index" | "timeindex")
}

/// The files `before` and `after` disagree on: those whose change is not
/// [`derived`], and those whose change is.
pub fn changed_files(
    before: &BTreeMap<String, Vec<u8>>,
    after: &BTreeMap<String, Vec<u8>>,
) -> (Vec<String>, Vec<String>) {
    let mut names: Vec<&String> = before.keys().chain(after.keys()).collect();
    names.sort_unstable();
    names.dedup();
    names
        .into_iter()
        .filter(|name| before.get(*name) != after.get(*name))
        .map(String::clone)
        .partition(|name| !derived(name))
}

/// The message the source must refuse the level with, given its declared
/// `metadata.version` range: Kafka's, from `FeatureControlManager.replay`.
pub fn refusal_message(range: LevelRange) -> String {
    format!(
        "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', \
         featureLevel={UNSTABLE_LEVEL}), but this controller only supports versions {range}"
    )
}

/// The one-partition path the record is written to and read back from.
fn guard_path(contract: &Contract) -> LoadPath {
    let mut path = contract.paths[0].clone();
    TOPIC.clone_into(&mut path.name);
    path.partitions = 1;
    path.key_space = None;
    path
}

/// Write one record, acknowledged by every replica.
async fn write_record(bootstrap: &str, path: &LoadPath) -> Result<(), String> {
    // The close ends the producer's block, so the producer is gone before the
    // write's outcome is returned. A failed close does not change that outcome.
    let (written, _closed) = {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_owned())
            .client_id("rollback-guard".to_owned())
            .acks(Acks::All)
            .enable_idempotence(true)
            .build()
            .await
            .map_err(|error| error.to_string())?;
        let name = producer_name(&path.name);
        let delivery = producer
            .enqueue(ProducerRecord {
                topic: path.name.clone(),
                value: Some(record_value(&name, 0, 64)),
                ..Default::default()
            })
            .await
            .map_err(|error| error.to_string())?;
        let written = delivery.await.map(drop).map_err(|error| error.to_string());
        (written, producer.close().await)
    };
    written
}

/// Stop and remove `broker`'s container, keeping its data.
fn take_down(broker: &SoakBroker) -> Result<(), String> {
    docker_try(&["stop", "--time", "60", &broker.name])?;
    docker_try(&["rm", &broker.name]).map(drop)
}

/// Start `broker` again from `image`.
fn bring_up(broker: &SoakBroker, image: &str) -> Result<(), String> {
    let argv = broker.launch.argv(image);
    docker_try(&argv.iter().map(String::as_str).collect::<Vec<_>>()).map(drop)
}

/// Wait for `broker`'s container to exit, and return its exit code, or `None`
/// when it is still running at the limit.
async fn exit_code(broker: &SoakBroker, limit: Duration) -> Option<i64> {
    let deadline = Instant::now() + limit;
    loop {
        let state = docker_try(&[
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.ExitCode}}",
            &broker.name,
        ])
        .unwrap_or_default();
        if let Some(("false", code)) = state.split_once(' ') {
            return code.trim().parse().ok();
        }
        if Instant::now() >= deadline {
            return None;
        }
        time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run the refused rollback of `contract`'s source under `target`.
pub async fn refuse_unsupported_rollback(
    contract: &Contract,
    target: &str,
) -> Result<RefusedRollback, String> {
    let lifecycle = contract
        .lifecycle
        .as_ref()
        .ok_or_else(|| "the contract declares no source release".to_owned())?;
    let range = *lifecycle
        .source
        .supports
        .get("metadata.version")
        .ok_or_else(|| "the source declares no metadata.version range".to_owned())?;
    let format_args = [
        "--release-version".to_owned(),
        UNSTABLE_RELEASE.to_owned(),
        "--unstable-feature-versions-enable".to_owned(),
    ];
    let cluster = SoakCluster::configured()
        .brokers(1)
        .image(target)
        .format_args(&format_args)
        .tables_toml(UNSTABLE_TOML.to_owned())
        .call();
    let broker = &cluster.brokers[0];
    let path = guard_path(contract);
    create_topics(&broker.bootstrap(), &[CreatableTopic {
        name: path.name.clone(),
        num_partitions: 1,
        replication_factor: 1,
        ..Default::default()
    }])
    .await?;
    write_record(&broker.bootstrap(), &path).await?;

    let config = broker.root.path().join("broker.toml");
    let unstable = fs::read_to_string(&config).map_err(|error| error.to_string())?;
    take_down(broker)?;
    let before = file_contents(&broker.host_log_dir());
    // The operator's rollback: the source, with its own configuration.
    fs::write(&config, unstable.replace(UNSTABLE_TOML, "")).map_err(|error| error.to_string())?;
    bring_up(broker, &source_image(lifecycle))?;
    let Some(source_exit_code) = exit_code(broker, REFUSAL_LIMIT).await else {
        let log = broker.container_log();
        let _ = take_down(broker);
        return Err(format!(
            "{} still runs {REFUSAL_LIMIT:?} after it was started over a log finalized at \
             metadata.version {UNSTABLE_LEVEL}:\n{log}",
            lifecycle.source.version
        ));
    };
    let log = broker.container_log();
    let refusal = refusal_message(range);
    let refused = log.contains(&refusal);
    docker_try(&["rm", &broker.name])?;
    let after = file_contents(&broker.host_log_dir());
    let (changed, derived_changed) = changed_files(&before, &after);
    if !changed.is_empty() {
        // The source's own log says what it wrote before it refused, and the
        // refusal message alone does not.
        eprintln!(
            "soak-contract: the refused rollback changed {changed:?}; {} logged:\n{log}",
            lifecycle.source.version
        );
    }

    // The candidate, with the switch on again, opens what the source left.
    fs::write(&config, unstable).map_err(|error| error.to_string())?;
    bring_up(broker, target)?;
    let reader = read_back(
        &Endpoint::plaintext(broker.bootstrap()),
        &path,
        Isolation::ReadUncommitted,
        &producer_name(&path.name),
    )
    .await;
    let outcome = RefusedRollback {
        source_exit_code,
        refusal: if refused { refusal } else { log },
        changed,
        derived_changed,
        record_read_back: reader.seen.contains(0),
    };
    if source_exit_code == 0 || !refused {
        return Err(format!(
            "{} did not refuse with `{}`: {outcome:#?}",
            lifecycle.source.version,
            refusal_message(range)
        ));
    }
    Ok(outcome)
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use {
        super::{changed_files, file_contents, refusal_message},
        assert2::check,
        krabka_bench_driver::soak::lifecycle::LevelRange,
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        std::{collections::BTreeMap, fs},
        tempfile::TempDir,
    };

    #[derive(DataTableRow)]
    struct FileImage {
        name: String,
        contents_hex: String,
    }

    #[derive(Default)]
    struct RollbackObservation {
        before: BTreeMap<String, Vec<u8>>,
        after: BTreeMap<String, Vec<u8>>,
        changed: Vec<String>,
        derived: Vec<String>,
        directory: Option<TempDir>,
        walked: BTreeMap<String, Vec<u8>>,
        range: Option<LevelRange>,
        refusal: Option<String>,
    }

    #[fixture]
    fn rollback() -> RollbackObservation {
        // Each rollback comparison retains its own byte images and observation.
        RollbackObservation {
            directory: None,
            ..Default::default()
        }
    }

    fn file_images(rows: Rows<FileImage>) -> BTreeMap<String, Vec<u8>> {
        rows.into_iter()
            .map(|row| {
                (
                    row.name,
                    hex::decode(row.contents_hex).expect("a hexadecimal file image"),
                )
            })
            .collect()
    }

    #[given("rollback files before opening are:")]
    fn before_open(rollback: &mut RollbackObservation, #[datatable] rows: Rows<FileImage>) {
        rollback.before = file_images(rows);
    }

    #[given("rollback files after opening are:")]
    fn after_open(rollback: &mut RollbackObservation, #[datatable] rows: Rows<FileImage>) {
        rollback.after = file_images(rows);
    }

    #[when("the rollback guard compares persisted and rebuildable file changes")]
    fn compare_file_images(rollback: &mut RollbackObservation) {
        (rollback.changed, rollback.derived) = changed_files(&rollback.before, &rollback.after);
    }

    fn file_names(names: &str) -> Vec<String> {
        if names == "none" {
            Vec::new()
        } else {
            names.split(',').map(str::to_owned).collect()
        }
    }

    #[then("persisted changes are {changed} and derived rebuilds are {derived}")]
    fn classified_file_changes(rollback: &RollbackObservation, changed: String, derived: String) {
        check!(rollback.changed == file_names(&changed));
        check!(rollback.derived == file_names(&derived));
    }

    #[given("a rollback data directory contains p-0/0.log with byte x")]
    fn persisted_directory(rollback: &mut RollbackObservation) {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("p-0")).unwrap();
        fs::write(directory.path().join("p-0").join("0.log"), b"x").unwrap();
        rollback.directory = Some(directory);
    }

    #[when("the rollback guard reads each file's bytes below the data directory")]
    fn read_persisted_files(rollback: &mut RollbackObservation) {
        rollback.walked = file_contents(
            rollback
                .directory
                .as_ref()
                .expect("the data directory exists")
                .path(),
        );
    }

    #[then("the byte image contains only p-0/0.log with x")]
    fn persisted_file_image(rollback: &RollbackObservation) {
        check!(rollback.walked == BTreeMap::from([("p-0/0.log".to_owned(), b"x".to_vec())]));
    }

    #[given("the rollback source supports feature levels seven through thirty")]
    fn rollback_source_range(rollback: &mut RollbackObservation) {
        rollback.range = Some(LevelRange { min: 7, max: 30 });
    }

    #[when("the rollback guard explains refusal of metadata.version thirty-three")]
    fn explain_rollback_refusal(rollback: &mut RollbackObservation) {
        rollback.refusal = Some(refusal_message(
            rollback.range.expect("the source range was supplied"),
        ));
    }

    #[then("the refusal uses Kafka's FeatureLevelRecord wording and the source range")]
    fn kafka_rollback_message(rollback: &RollbackObservation) {
        check!(
            rollback.refusal.as_deref()
                == Some(
                    "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', featureLevel=33), but this controller only supports versions 7-30"
                )
        );
    }

    scenarios!("tests/features/migrated/tests/soak_contract/rollback_guard.feature", fixtures = [rollback: RollbackObservation]);
}
