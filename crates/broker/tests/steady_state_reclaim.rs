//! Two directories that grow under steady load until a segment rolls, and the
//! settings that make the soak lane's cluster roll them inside its run.
//!
//! A steady consumer group commits its offsets every few seconds, and the
//! metadata quorum's leader appends a KIP-835 `NoOpRecord` every
//! `metadata.max.idle.interval.ms`. Neither write is ever reclaimed from the
//! active segment. The cleaner never compacts the active segment of
//! `__consumer_offsets`, and the metadata log gives up only whole segments
//! below its log start. At Kafka's defaults both segments roll after seven
//! days, or at 100 MiB and 1 GiB. So for the length of any soak, both
//! directories only climb. Kafka does the same, and it is not a leak.
//!
//! That climb failed the four-hour soak's log-directory drift bound once
//! #1303 stopped every offset commit from rolling a segment and #1307 adopted
//! Kafka's metadata-log retention and KIP-835 no-ops. The soak now sets
//! `log.roll.ms` and the metadata log's roll, snapshot and retention intervals
//! to one minute (`STEADY_STATE_TABLES` in `soak.rs`), so both directories roll and are
//! reclaimed inside the run, and the drift bound judges reclamation rather
//! than the calendar.
//!
//! This test is that finding in seconds instead of hours. The same committer
//! and the same no-op rate run twice, once at Kafka's defaults and once at the
//! soak's settings scaled down from a minute to [`ROLL`]. At the defaults each
//! directory's second-half rise is everything the second half wrote. At the
//! soak's settings it is a fraction of that.

mod support;

use {
    assert2::check,
    krabka_broker::{Broker, BrokerConfig},
    krabka_units::prelude::{Time, millis},
    std::{path::Path, time::Duration},
    support::{client::connect_owned, commit_partition_zero, create_topic_for_commits},
};

/// The soak's one-minute roll, snapshot and retention intervals, scaled down.
const ROLL: Time = millis(100);

/// How often the cleaner sweeps. Short beside [`ROLL`], as the soak's one
/// second is beside its minute.
const CLEANER_INTERVAL: Time = millis(25);

/// `metadata.max.idle.interval.ms` in both runs: the steady metadata writer.
const IDLE_INTERVAL: Time = millis(10);

/// Commits in each half of a run.
const COMMITS_PER_HALF: i64 = 300;

/// Commits between two samples of the directories: fifteen samples a half.
const COMMITS_PER_SAMPLE: i64 = 20;

/// The pause between commits.
const COMMIT_PACE: Duration = Duration::from_millis(10);

/// The topic whose offsets the group commits.
const TOPIC: &str = "steady-commits";

/// The group that commits them.
const GROUP: &str = "steady-committer";

/// The directory prefix of the offsets topic's partitions.
const OFFSETS_PREFIX: &str = "__consumer_offsets-";

/// How a run's broker is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settings {
    /// Kafka's defaults: seven-day rolls, a one-hour snapshot interval and
    /// 100 MiB / seven-day metadata retention.
    KafkaDefaults,
    /// The soak's settings, at [`ROLL`] in place of one minute.
    Soak,
}

impl Settings {
    fn configure(self, config: &mut BrokerConfig) {
        config.cleaner_interval = CLEANER_INTERVAL;
        config.metadata_log.max_idle_interval = IDLE_INTERVAL;
        if self == Self::Soak {
            // `log.roll.ms`: the offsets topic sets no `segment.ms` of its own.
            config.log_config.segment_roll_interval = ROLL;
            config.metadata_log.segment_roll_interval = ROLL;
            config.metadata_max_snapshot_interval = ROLL;
            config.metadata_log.max_retention = Some(ROLL);
        }
    }
}

/// Bytes of the live files in the top-level entries of `log_dir` whose names
/// start with `prefix`, one level deep: a partition directory holds no
/// subdirectory.
///
/// A retired file, renamed to `.deleted`, is left out. It waits out
/// `file.delete.delay.ms`, which is a fixed minute on the metadata log, as in
/// Kafka, and so outlasts this run. `krabka-log` and `KraftLog` test that it
/// is then deleted, and the soak, at four hours, counts it.
fn prefixed_bytes(log_dir: &Path, prefix: &str) -> u64 {
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .filter_map(|entry| std::fs::read_dir(entry.path()).ok())
        .flat_map(Iterator::flatten)
        .filter(|file| !file.file_name().to_string_lossy().ends_with(".deleted"))
        .filter_map(|file| file.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|metadata| metadata.len())
        .sum()
}

/// The two directories' sizes at one instant.
#[derive(Debug, Clone, Copy)]
struct Sizes {
    offsets: u64,
    metadata: u64,
}

impl Sizes {
    fn of(log_dir: &Path) -> Self {
        Self {
            offsets: prefixed_bytes(log_dir, OFFSETS_PREFIX),
            metadata: prefixed_bytes(log_dir, krabka_raft::METADATA_PARTITION_DIR),
        }
    }
}

/// The median of `values`, which must not be empty.
fn median(mut values: Vec<u64>) -> i128 {
    values.sort_unstable();
    i128::from(values[values.len() / 2])
}

/// How much each directory rose from the first half of a run to the second,
/// as the difference of the two halves' medians.
///
/// A directory that only climbs rises by about a half's writes. One that a
/// roll reclaims is a sawtooth, and a single sample lands anywhere on a tooth:
/// on a loaded runner a lagging cleaning made an end-minus-midpoint reading
/// of the metadata log a third of the climb it was compared against. The
/// median of fifteen samples sits mid-tooth in both halves, so a sawtooth's
/// rise stays near zero however its phase falls.
#[derive(Debug, Clone, Copy)]
struct Rise {
    offsets: i128,
    metadata: i128,
}

/// Run the committer for two halves on a broker set up by `settings`, and
/// return each directory's rise from the first half to the second.
async fn second_half_rise(settings: Settings) -> Rise {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    settings.configure(&mut config);
    let log_dir = config.log_dir.clone();
    let broker = Box::pin(Broker::start(config)).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let client = connect_owned(broker.listen_addr().to_string(), "steady-reclaim", "client").await;

    let topic_id = create_topic_for_commits(&client, TOPIC).await;

    let mut halves: [Vec<Sizes>; 2] = [Vec::new(), Vec::new()];
    for (half, samples) in (0..).zip(&mut halves) {
        for commit in 0..COMMITS_PER_HALF {
            let offset = half * COMMITS_PER_HALF + commit;
            commit_partition_zero(&client, GROUP, (TOPIC, topic_id), offset).await;
            tokio::time::sleep(COMMIT_PACE).await;
            if (commit + 1) % COMMITS_PER_SAMPLE == 0 {
                samples.push(Sizes::of(&log_dir));
            }
        }
    }
    broker.shutdown().await;

    let [first, second] = halves;
    let rise = |field: fn(&Sizes) -> u64| {
        median(second.iter().map(field).collect()) - median(first.iter().map(field).collect())
    };
    Rise {
        offsets: rise(|sizes| sizes.offsets),
        metadata: rise(|sizes| sizes.metadata),
    }
}

/// At Kafka's defaults a steady committer and a steady no-op writer each
/// climb by a half's writes from one half to the next. At the soak's roll,
/// snapshot and retention settings, both directories rise by less than a
/// quarter of that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_soak_settings_reclaim_what_kafkas_defaults_let_climb() {
    let defaults = second_half_rise(Settings::KafkaDefaults).await;
    let soak = second_half_rise(Settings::Soak).await;

    let rows = [
        ("__consumer_offsets", defaults.offsets, soak.offsets),
        ("__cluster_metadata-0", defaults.metadata, soak.metadata),
    ];
    for (directory, at_defaults, at_soak) in rows {
        eprintln!(
            "{directory}: a median rise of {at_defaults} bytes at Kafka's defaults, \
             {at_soak} at the soak's settings"
        );
        check!(
            at_defaults > 0,
            "{directory} should climb at Kafka's defaults, and rose {at_defaults} bytes"
        );
        check!(
            at_soak * 4 < at_defaults,
            "{directory} rose {at_soak} bytes at the soak's settings against \
             {at_defaults} at Kafka's defaults: rolling it did not reclaim it"
        );
    }
}
