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
        path::Path,
        time::{Duration, Instant},
    },
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
pub(crate) struct RefusedRollback {
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
pub(crate) fn file_contents(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut contents = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
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
pub(crate) fn changed_files(
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
pub(crate) fn refusal_message(range: LevelRange) -> String {
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
    let _ = producer.close().await;
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run the refused rollback of `contract`'s source under `target`.
pub(crate) async fn refuse_unsupported_rollback(
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
    let unstable = std::fs::read_to_string(&config).map_err(|error| error.to_string())?;
    take_down(broker)?;
    let before = file_contents(&broker.host_log_dir());
    // The operator's rollback: the source, with its own configuration.
    std::fs::write(&config, unstable.replace(UNSTABLE_TOML, ""))
        .map_err(|error| error.to_string())?;
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

    // The candidate, with the switch on again, opens what the source left.
    std::fs::write(&config, unstable).map_err(|error| error.to_string())?;
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
mod tests {
    use {
        super::{changed_files, file_contents, refusal_message},
        assert2::check,
        krabka_bench_driver::soak::lifecycle::LevelRange,
        std::collections::BTreeMap,
    };

    fn files(files: &[(&str, &[u8])]) -> BTreeMap<String, Vec<u8>> {
        files
            .iter()
            .map(|(name, bytes)| ((*name).to_owned(), bytes.to_vec()))
            .collect()
    }

    #[yare::parameterized(
        untouched = { &[("0.log", b"ab")], &[("0.log", b"ab")], &[], &[] },
        snapshot_removed = { &[("0.log", b"ab"), ("55.snapshot", b"s")], &[("0.log", b"ab")], &[], &["55.snapshot"] },
        time_index_rebuilt = { &[("0.timeindex", b"")], &[("0.timeindex", b"entry")], &[], &["0.timeindex"] },
        offset_index_trimmed = { &[("0.index", b"ab\0")], &[("0.index", b"ab")], &[], &["0.index"] },
        checkpoint_rewritten = { &[("0-0.checkpoint", b"ab")], &[("0-0.checkpoint", b"ac")], &["0-0.checkpoint"], &[] },
        segment_appended = { &[("0.log", b"ab")], &[("0.log", b"ab\0")], &["0.log"], &[] },
        quorum_state_written = { &[], &[("quorum-state", b"{}")], &["quorum-state"], &[] },
    )]
    fn only_what_an_open_rebuilds_may_change(
        before: &[(&str, &[u8])],
        after: &[(&str, &[u8])],
        changed: &[&str],
        derived: &[&str],
    ) {
        let (found, found_derived) = changed_files(&files(before), &files(after));
        check!(
            found
                == changed
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<_>>()
        );
        check!(
            found_derived
                == derived
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_directory_is_read_file_by_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("p-0")).unwrap();
        std::fs::write(dir.path().join("p-0").join("0.log"), b"x").unwrap();
        check!(
            file_contents(dir.path()) == BTreeMap::from([("p-0/0.log".to_owned(), b"x".to_vec())])
        );
    }

    #[test]
    fn the_refusal_is_kafkas_with_the_sources_range() {
        check!(
            refusal_message(LevelRange { min: 7, max: 30 })
                == "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', \
                    featureLevel=33), but this controller only supports versions 7-30"
        );
    }
}
