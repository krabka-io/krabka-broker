//! KIP-1066 cordoned log directories: the `cordoned.log.dirs` broker config,
//! the set of directories this broker holds cordoned, and the rules that read
//! it.
//!
//! A cordoned directory keeps serving the replicas it holds but takes no new
//! one. Kafka 4.3.1 applies the set in four places, and so does krabka:
//!
//! - New partition directories go to an uncordoned directory
//!   (`LogManager.nextLogDirs`). [`CordonedLogDirs::placement_dirs`] is that
//!   filter.
//! - `AlterReplicaLogDirs` refuses a cordoned destination with
//!   `INVALID_REPLICA_ASSIGNMENT` (`ReplicaManager.alterReplicaLogDirs`).
//! - `DescribeLogDirs` v5 reports `IsCordoned`, from `metadata.version`
//!   `4.3-IV0` on (`ReplicaManager.describeLogDirs`).
//! - The broker reports the directory ids in `BrokerHeartbeat`, and the
//!   controller keeps them on the registration. Automatic replica placement
//!   leaves out a broker whose directories are all cordoned
//!   (`BrokerRegistration.hasUncordonedDirs`); [`has_uncordoned_dirs`] is that
//!   test.
//!
//! The config is a Kafka `LIST` of entries of `log.dirs`, or `*` for all of
//! them. It is dynamic, per broker only
//! (`DynamicBrokerConfig.PER_BROKER_CONFIGS`), and the per-broker dynamic
//! value overrides the static one. An update Kafka cannot apply leaves the
//! previous set in force, as `DynamicBrokerConfig.updateBrokerConfig` does.
//!
//! Kafka 4.3.1 does not refuse a manual assignment, or a reassignment target,
//! that names a broker whose directories are all cordoned. That refusal is
//! KAFKA-20832, which is on Kafka trunk and the 4.3 branch after 4.3.1.

use {
    crate::metadata_source::{MetadataSource, watch_image_loop},
    arc_swap::ArcSwap,
    krabka_metadata::{
        BrokerRegistrationChangeRecord, BrokerRegistrationRecord, MetadataImage, NodeId,
    },
    std::{
        collections::{BTreeSet, HashSet},
        path::{Path, PathBuf},
        sync::Arc,
    },
    tokio_util::sync::CancellationToken,
};

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG`.
pub const CORDONED_LOG_DIRS: &str = "cordoned.log.dirs";

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_ALL`: cordon every directory.
pub const CORDONED_LOG_DIRS_ALL: &str = "*";

/// Kafka's `ServerLogConfigs.CORDONED_LOG_DIRS_DOC`.
pub const CORDONED_LOG_DIRS_DOC: &str = "A comma-separated list of the directories that \
                                                are cordoned. Entries in this list must be \
                                                entries in log.dirs or log.dir configuration. \
                                                This can also be set to * to cordon all log \
                                                directories.";

/// Parse a `LIST` value as Kafka's `ConfigDef.parseType` does: a blank value
/// is the empty list, and anything else splits on commas with the whitespace
/// around them removed. Kafka then drops duplicates (`ConfigDef.parseValue`).
fn parse_list(value: &str) -> Vec<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut entries: Vec<&str> = Vec::new();
    for entry in trimmed.split(',').map(str::trim) {
        if !entries.contains(&entry) {
            entries.push(entry);
        }
    }
    entries
}

/// Kafka's `ValidList.anyNonDuplicateValues(true, true)` on the value, which
/// `DynamicBrokerConfig.validateConfigTypes` applies before anything else.
///
/// # Errors
/// Returns Kafka's message when an entry is empty.
pub fn validate_value_type(value: &str) -> Result<(), String> {
    if parse_list(value).iter().any(|entry| entry.is_empty()) {
        return Err(format!(
            "Configuration '{CORDONED_LOG_DIRS}' values must not be empty."
        ));
    }
    Ok(())
}

/// The configured string of each log directory, which is what Kafka compares
/// a `cordoned.log.dirs` entry against (`AbstractKafkaConfig.logDirs`).
fn configured_names(log_dirs: &[PathBuf]) -> Vec<String> {
    log_dirs
        .iter()
        .map(|dir| dir.display().to_string())
        .collect()
}

/// Kafka's type check followed by `KafkaConfig.validateCordonedLogDirs`, the
/// check a broker applies to its own value at startup and to every dynamic
/// update of it. It returns the directories the value cordons.
///
/// # Errors
/// Returns Kafka's message: an empty entry, `*` beside another entry, or an
/// entry that names no directory in `log_dirs`. The last two carry the
/// `requirement failed: ` prefix of Scala's `require`, which is what
/// `kafka-configs` prints.
pub fn resolve(value: &str, log_dirs: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    validate_value_type(value)?;
    let entries = parse_list(value);
    if entries.contains(&CORDONED_LOG_DIRS_ALL) {
        if entries.len() != 1 {
            return Err(format!(
                "requirement failed: When {CORDONED_LOG_DIRS} is set to {CORDONED_LOG_DIRS_ALL}, \
                 it must not contain other values"
            ));
        }
        return Ok(log_dirs.to_vec());
    }
    let names = configured_names(log_dirs);
    let missing: Vec<&str> = entries
        .iter()
        .copied()
        .filter(|entry| !names.iter().any(|name| name == entry))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "requirement failed: All entries in {CORDONED_LOG_DIRS} must be present in log.dirs \
             or log.dir. Missing entries : {}",
            missing.join(", ")
        ));
    }
    Ok(log_dirs
        .iter()
        .zip(&names)
        .filter(|(_, name)| entries.contains(&name.as_str()))
        .map(|(dir, _)| dir.clone())
        .collect())
}

/// Kafka's `BrokerRegistration.hasUncordonedDirs`: whether automatic replica
/// placement may use the broker. A broker that has not reported its cordoned
/// directories yet, or that registered no directory ids, counts as usable.
pub fn has_uncordoned_dirs(registration: &BrokerRegistrationRecord) -> bool {
    let Some(cordoned) = &registration.cordoned_log_dirs else {
        return true;
    };
    registration.log_dirs.is_empty()
        || registration
            .log_dirs
            .iter()
            .any(|dir| !cordoned.contains(dir))
}

/// The registered brokers whose directories are all cordoned, which the
/// automatic placement of `CreateTopics` and `CreatePartitions` leaves out.
pub fn fully_cordoned_brokers(image: &MetadataImage) -> HashSet<u64> {
    image
        .brokers()
        .filter(|registration| !has_uncordoned_dirs(registration))
        .map(|registration| registration.node_id.0)
        .collect()
}

/// The `BrokerRegistrationChangeRecord` that stores `reported` as the cordoned
/// directories of `node_id`, at its broker epoch, or `None` when the
/// registration already holds that set.
///
/// This is Kafka's `ReplicationControlManager.handleDirectoriesCordoned`. A
/// heartbeat with no cordoned directories (the broker has not caught up yet)
/// changes nothing. A registration that holds none yet always takes the
/// reported set, and otherwise the two are compared as sets
/// (`BrokerRegistration.cordonedDirChanged`).
pub fn registration_change(
    image: &MetadataImage,
    node_id: NodeId,
    reported: Option<&[uuid::Uuid]>,
) -> Option<krabka_metadata::MetadataRecord> {
    let reported = reported?;
    let current = image.broker(node_id)?;
    let changed = current.cordoned_log_dirs.as_ref().is_none_or(|stored| {
        stored.iter().collect::<BTreeSet<_>>() != reported.iter().collect::<BTreeSet<_>>()
    });
    changed.then(|| {
        krabka_metadata::MetadataRecord::V1BrokerRegistrationChange(
            BrokerRegistrationChangeRecord {
                cordoned_log_dirs: Some(reported.to_vec()),
                ..BrokerRegistrationChangeRecord::no_change(node_id, current.broker_epoch)
            },
        )
    })
}

/// The directories this broker holds cordoned, shared by every reader.
///
/// Kafka keeps the set on `LogManager` and `DynamicLogConfig.reconfigure`
/// replaces it. Here an image watcher replaces it with
/// [`CordonedLogDirs::apply_image`], and the placement, `AlterReplicaLogDirs`,
/// `DescribeLogDirs` and the heartbeat all read the same set.
#[derive(Clone, Debug, Default)]
pub struct CordonedLogDirs {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Every configured log directory, primary first.
    log_dirs: Vec<PathBuf>,
    /// The static `cordoned.log.dirs`, as the operator wrote it.
    static_value: Option<String>,
    /// The set in force.
    current: ArcSwap<Vec<PathBuf>>,
}

impl CordonedLogDirs {
    /// The set a broker starts with: its static `cordoned.log.dirs`. The
    /// broker has already refused a static value that does not resolve, so an
    /// unresolvable one here cordons nothing.
    #[must_use]
    pub(crate) fn new(log_dirs: Vec<PathBuf>, static_value: Option<String>) -> Self {
        let current = static_value
            .as_deref()
            .and_then(|value| resolve(value, &log_dirs).ok())
            .unwrap_or_default();
        Self {
            inner: Arc::new(Inner {
                log_dirs,
                static_value,
                current: ArcSwap::from_pointee(current),
            }),
        }
    }

    /// Whether `dir`, one of the configured log directories, is cordoned.
    #[must_use]
    pub(crate) fn is_cordoned(&self, dir: &Path) -> bool {
        self.inner
            .current
            .load()
            .iter()
            .any(|cordoned| cordoned == dir)
    }

    /// The cordoned directories, in configuration order.
    #[must_use]
    pub(crate) fn cordoned(&self) -> Vec<PathBuf> { self.inner.current.load().as_ref().clone() }

    /// The directories of `candidates` a new partition may go to: the
    /// uncordoned ones, or the first candidate when every one is cordoned.
    ///
    /// Kafka's `LogManager.nextLogDirs` falls back the same way, because the
    /// controller may have assigned the replica to this broker just before
    /// its last directory was cordoned.
    #[must_use]
    pub(crate) fn placement_dirs(&self, candidates: &[PathBuf]) -> Vec<PathBuf> {
        let open: Vec<PathBuf> = candidates
            .iter()
            .filter(|dir| !self.is_cordoned(dir))
            .cloned()
            .collect();
        if open.is_empty() {
            candidates.first().cloned().into_iter().collect()
        } else {
            open
        }
    }

    /// Replace the set with the one `image` gives this broker: its per-broker
    /// dynamic `cordoned.log.dirs` when it holds one, and its static value
    /// otherwise.
    ///
    /// A dynamic value that does not resolve leaves the set as it was, as a
    /// Kafka broker keeps its previous configuration when
    /// `DynamicBrokerConfig.updateBrokerConfig` cannot apply an update.
    pub(crate) fn apply_image(&self, image: &MetadataImage, node_id: NodeId) {
        let dynamic = image
            .broker_config(node_id)
            .and_then(|configs| configs.get(CORDONED_LOG_DIRS));
        let value = dynamic
            .map(String::as_str)
            .or(self.inner.static_value.as_deref());
        let next = match value {
            None => Vec::new(),
            Some(value) => match resolve(value, &self.inner.log_dirs) {
                Ok(next) => next,
                Err(reason) => {
                    tracing::error!(
                        %reason,
                        value,
                        "{CORDONED_LOG_DIRS} could not be applied; keeping the previous value"
                    );
                    return;
                }
            },
        };
        if *self.inner.current.load().as_ref() != next {
            tracing::info!(cordoned = ?next, "{CORDONED_LOG_DIRS} updated");
            self.inner.current.store(Arc::new(next));
        }
    }
}

/// Keep `cordoned` in step with the metadata image until `shutdown`.
pub fn spawn_watcher(
    controller: &Arc<dyn MetadataSource>,
    node_id: NodeId,
    cordoned: CordonedLogDirs,
    shutdown: CancellationToken,
) {
    tokio::spawn(watch_image_loop(
        controller.watch_image(),
        "cordoned log dirs",
        shutdown,
        move |image| cordoned.apply_image(image, node_id),
    ));
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{future_log::test_support::step_list, test_support::broker_registration},
        assert2::{assert, check},
        krabka_bdd::{Given, given, then, when},
        krabka_metadata::{BrokerConfigRecord, MetadataRecord},
        std::str::FromStr,
    };

    fn dirs() -> Vec<PathBuf> { vec![PathBuf::from("/d1"), PathBuf::from("/d2")] }

    #[derive(Default)]
    struct CordoningRequest {
        directories: Vec<PathBuf>,
        input: String,
        resolved: Option<Result<Vec<PathBuf>, String>>,
    }

    #[rstest::fixture]
    fn cordoning_request() -> CordoningRequest {
        // Directory resolution receives fresh configured paths for each request.
        CordoningRequest::default()
    }

    #[rstest_bdd_macros::given("the broker log directories are /d1 and /d2")]
    fn configured_cordon_directories(cordoning_request: &mut CordoningRequest) {
        cordoning_request.directories = dirs();
    }

    #[rstest_bdd_macros::given("cordoned.log.dirs is JSON {value}")]
    fn supplied_cordoning_value(cordoning_request: &mut CordoningRequest, value: String) {
        cordoning_request.input =
            serde_json::from_str(&value).expect("JSON preserves directory whitespace");
    }

    #[rstest_bdd_macros::when("the cordoned log directories are resolved")]
    fn resolve_cordoning_value(cordoning_request: &mut CordoningRequest) {
        cordoning_request.resolved = Some(resolve(
            &cordoning_request.input,
            &cordoning_request.directories,
        ));
    }

    #[rstest_bdd_macros::then("the ordered cordoned directories are JSON {paths}")]
    fn expected_cordoned_directories(cordoning_request: &CordoningRequest, paths: String) {
        let expected: Vec<PathBuf> = serde_json::from_str(&paths).expect("explicit directory list");
        check!(cordoning_request.resolved == Some(Ok(expected)));
    }

    #[rstest_bdd_macros::then("the cordoning request fails with {message}")]
    fn refused_cordoning_request(cordoning_request: &CordoningRequest, message: String) {
        check!(cordoning_request.resolved == Some(Err(message)));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/cordoned_log_dirs.feature",
        fixtures = [cordoning_request: CordoningRequest, cordoned_broker: CordonedBrokerScenario]
    );

    /// A set of directory ids, written as letters: `a`, `b`, or `nothing`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DirectoryIds(Vec<uuid::Uuid>);

    impl FromStr for DirectoryIds {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            step_list(text, |letter| match letter {
                "a" => Ok(uuid::Uuid::from_u128(1)),
                "b" => Ok(uuid::Uuid::from_u128(2)),
                other => Err(format!("{other} is not a directory letter")),
            })
            .map(Self)
        }
    }

    /// Configured log directories, written as `/d1,/d2` or `nothing`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DirectoryPaths(Vec<PathBuf>);

    impl FromStr for DirectoryPaths {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            step_list(text, |path| Ok(PathBuf::from(path))).map(Self)
        }
    }

    /// A `cordoned.log.dirs` value, where `empty` is the empty string.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CordoningValue(String);

    impl FromStr for CordoningValue {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(if text == "empty" {
                String::new()
            } else {
                text.to_owned()
            }))
        }
    }

    /// The broker every cordoning-state scenario is about.
    const BROKER: NodeId = NodeId(1);

    struct CordonedBrokerScenario {
        cordoned: Given<CordonedLogDirs>,
        log_dirs: Given<Vec<uuid::Uuid>>,
        reported: Option<Vec<uuid::Uuid>>,
        usable: Given<bool>,
        change: Given<Option<MetadataRecord>>,
    }

    #[rstest::fixture]
    fn cordoned_broker() -> CordonedBrokerScenario {
        CordonedBrokerScenario {
            cordoned: Given::new(
                "cordoned",
                "Given a broker with log directories /d1 and /d2 starts with cordoned.log.dirs \
                 <static>",
            ),
            log_dirs: Given::new(
                "log_dirs",
                "Given a broker registered log directories <registered>",
            ),
            reported: None,
            usable: Given::new(
                "usable",
                "When automatic replica placement considers the broker",
            ),
            change: Given::new(
                "change",
                "When the broker heartbeats with <reported> cordoned",
            ),
        }
    }

    impl CordonedBrokerScenario {
        /// The registration the controller holds for the broker.
        fn registration(&self) -> BrokerRegistrationRecord {
            BrokerRegistrationRecord {
                cordoned_log_dirs: self.reported.clone(),
                broker_epoch: 7,
                incarnation_id: uuid::Uuid::from_u128(1),
                log_dirs: self.log_dirs.get().clone(),
                ..broker_registration(BROKER)
            }
        }

        fn heartbeat(&mut self, reported: Option<&[uuid::Uuid]>) {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            image.apply(&MetadataRecord::V1BrokerRegistration(self.registration()));
            self.change
                .set(registration_change(&image, BROKER, reported));
        }
    }

    fn image_with(value: Option<&str>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(value) = value {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: BROKER,
                config_name: CORDONED_LOG_DIRS.into(),
                config_value: Some(value.into()),
            }));
        }
        image
    }

    #[given("a broker with log directories \\/d1 and \\/d2 starts with cordoned.log.dirs {value}")]
    fn broker_with_static_value(cordoned_broker: &mut CordonedBrokerScenario, value: String) {
        cordoned_broker
            .cordoned
            .set(CordonedLogDirs::new(dirs(), Some(value)));
    }

    #[given("its dynamic cordoned.log.dirs has been set to {value}")]
    fn dynamic_value_was_set(cordoned_broker: &mut CordonedBrokerScenario, value: String) {
        cordoned_broker
            .cordoned
            .get()
            .apply_image(&image_with(Some(&value)), BROKER);
    }

    #[when("its dynamic cordoned.log.dirs becomes {value}")]
    fn dynamic_value_becomes(cordoned_broker: &mut CordonedBrokerScenario, value: CordoningValue) {
        cordoned_broker
            .cordoned
            .get()
            .apply_image(&image_with(Some(&value.0)), BROKER);
    }

    #[when("its dynamic cordoned.log.dirs is removed")]
    fn dynamic_value_removed(cordoned_broker: &mut CordonedBrokerScenario) {
        cordoned_broker
            .cordoned
            .get()
            .apply_image(&image_with(None), BROKER);
    }

    #[then("the broker cordons {cordoned}")]
    fn broker_cordons(cordoned_broker: &CordonedBrokerScenario, cordoned: DirectoryPaths) {
        assert!(cordoned_broker.cordoned.get().cordoned() == cordoned.0);
    }

    #[given("a broker registered log directories {registered}")]
    fn broker_registered(cordoned_broker: &mut CordonedBrokerScenario, registered: DirectoryIds) {
        cordoned_broker.log_dirs.set(registered.0);
    }

    #[given("the broker reported {cordoned} as cordoned")]
    fn broker_reported(cordoned_broker: &mut CordonedBrokerScenario, cordoned: DirectoryIds) {
        cordoned_broker.reported = Some(cordoned.0);
    }

    #[when("automatic replica placement considers the broker")]
    fn placement_considers_broker(cordoned_broker: &mut CordonedBrokerScenario) {
        let usable = has_uncordoned_dirs(&cordoned_broker.registration());
        cordoned_broker.usable.set(usable);
    }

    #[then("the broker can take new replicas")]
    fn broker_can_take_replicas(cordoned_broker: &CordonedBrokerScenario) {
        assert!(*cordoned_broker.usable.get());
    }

    #[then("the broker cannot take new replicas")]
    fn broker_cannot_take_replicas(cordoned_broker: &CordonedBrokerScenario) {
        assert!(!*cordoned_broker.usable.get());
    }

    #[when("the broker heartbeats with {reported} cordoned")]
    fn heartbeat_with_set(cordoned_broker: &mut CordonedBrokerScenario, reported: DirectoryIds) {
        cordoned_broker.heartbeat(Some(&reported.0));
    }

    #[when("the broker heartbeats without a cordoned set")]
    fn heartbeat_without_set(cordoned_broker: &mut CordonedBrokerScenario) {
        cordoned_broker.heartbeat(None);
    }

    #[then("the controller records {reported} as the broker's cordoned directories")]
    fn controller_records_set(cordoned_broker: &CordonedBrokerScenario, reported: DirectoryIds) {
        let expected = MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            node_id: BROKER,
            broker_epoch: 7,
            fenced: krabka_metadata::FencingChange::None,
            in_controlled_shutdown: false,
            log_dirs: vec![],
            cordoned_log_dirs: Some(reported.0),
        });
        assert!(*cordoned_broker.change.get() == Some(expected));
    }

    #[then("the controller records no registration change")]
    fn controller_records_nothing(cordoned_broker: &CordonedBrokerScenario) {
        assert!(*cordoned_broker.change.get() == None);
    }

    /// Kafka's `LogManager.nextLogDirs`: uncordoned directories only, or the
    /// first one when all are cordoned.
    #[test]
    fn placement_skips_cordoned_directories_until_none_is_left() {
        let cordoned = CordonedLogDirs::new(dirs(), Some("/d1".into()));
        assert!(cordoned.placement_dirs(&dirs()) == vec![PathBuf::from("/d2")]);
        check!(cordoned.is_cordoned(Path::new("/d1")));
        check!(!cordoned.is_cordoned(Path::new("/d2")));

        cordoned.apply_image(&image_with(Some("*")), NodeId(1));
        assert!(cordoned.placement_dirs(&dirs()) == vec![PathBuf::from("/d1")]);
        assert!(cordoned.placement_dirs(&[]) == Vec::<PathBuf>::new());
    }
}
