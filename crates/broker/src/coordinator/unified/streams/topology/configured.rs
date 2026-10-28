//! Topology configuration: the task count of every subtopology, the partition
//! count of every internal topic, and the status that keeps a group
//! `NotReady`.
//!
//! This module ports Kafka's `InternalTopicManager.configureTopics` with its
//! helpers `RepartitionTopics`, `CopartitionedTopicsEnforcer` and
//! `ChangelogTopics`. The rules, their order, the status codes and the detail
//! strings are Kafka's:
//!
//! 1. A missing source topic stops the configuration with one
//!    `MISSING_SOURCE_TOPICS` status.
//! 2. A repartition topic with no explicit partition count gets the largest
//!    partition count of the source and repartition source topics of the
//!    subtopology that writes it.
//! 3. In a copartition group, the flexible repartition topics get the
//!    partition count of the external topics, which must all agree, or the
//!    count of the fixed repartition topics.
//! 4. A changelog topic gets the largest partition count of the source and
//!    repartition source topics of its subtopology.
//! 5. An internal topic that exists with another partition count is
//!    `INCORRECTLY_PARTITIONED_TOPICS`, and the internal topics that do not
//!    exist are `MISSING_INTERNAL_TOPICS`.
//!
//! Where Kafka iterates a `HashMap` or a `HashSet`, this port iterates in name
//! order, so that a message that names one of several topics is deterministic.

use {
    super::{metadata_hash::required_topics, status},
    crate::{
        codes,
        coordinator::unified::streams::{
            persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
            state::TopologyStatus,
        },
    },
    krabka_log::topic_name::validate_topic_name,
    krabka_metadata::MetadataImage,
    std::collections::{BTreeMap, BTreeSet},
};

/// Kafka's `ConfiguredInternalTopic`: a repartition or changelog topic with
/// its decided partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredInternalTopic {
    pub name: String,
    pub partitions: i32,
    /// The replication factor of the topology, or `None` when the topology
    /// leaves it at 0.
    pub replication_factor: Option<i16>,
    pub configs: BTreeMap<String, String>,
}

/// Kafka's `ConfiguredSubtopology`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredSubtopology {
    pub number_of_tasks: i32,
    pub source_topics: BTreeSet<String>,
    pub repartition_source_topics: BTreeMap<String, ConfiguredInternalTopic>,
    pub repartition_sink_topics: BTreeSet<String>,
    pub state_changelog_topics: BTreeMap<String, ConfiguredInternalTopic>,
}

/// Kafka's `ConfiguredTopology`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredTopology {
    pub topology_epoch: i32,
    /// The configured subtopologies by id. `None` when a status stopped the
    /// configuration before the subtopologies were sized.
    pub subtopologies: Option<BTreeMap<String, ConfiguredSubtopology>>,
    /// The internal topics that the metadata image does not hold, by name.
    pub internal_topics_to_create: BTreeMap<String, ConfiguredInternalTopic>,
    /// The status code and detail that keep the group `NotReady`: Kafka's
    /// `topicConfigurationException`.
    pub status: Option<TopologyStatus>,
}

impl ConfiguredTopology {
    /// Kafka's `isReady`: no status keeps the group from an assignment.
    #[must_use]
    pub fn is_ready(&self) -> bool { self.status.is_none() }

    /// The number of tasks of each configured subtopology, by id. Empty when
    /// the subtopologies were not sized.
    #[must_use]
    pub fn number_of_tasks(&self) -> BTreeMap<String, i32> {
        self.subtopologies
            .iter()
            .flatten()
            .map(|(id, subtopology)| (id.clone(), subtopology.number_of_tasks))
            .collect()
    }
}

/// A topology that cannot be configured. The heartbeat answers with the error
/// code and message, and it changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigureTopicsError {
    /// Kafka's `StreamsInvalidTopologyException`.
    #[error("{0}")]
    InvalidTopology(String),
    /// Kafka's `InvalidTopicException` from `Topic.validate` on an internal
    /// topic name.
    #[error("{0}")]
    InvalidTopic(String),
    /// An exception that Kafka does not map to a code (an
    /// `IllegalStateException` or an `IllegalArgumentException`). Kafka
    /// answers `UNKNOWN_SERVER_ERROR` with no message.
    #[error("{0}")]
    Internal(String),
}

impl ConfigureTopicsError {
    /// The wire error code of the heartbeat response.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        match self {
            Self::InvalidTopology(_) => codes::STREAMS_INVALID_TOPOLOGY,
            Self::InvalidTopic(_) => codes::INVALID_TOPIC_EXCEPTION,
            Self::Internal(_) => codes::UNKNOWN_SERVER_ERROR,
        }
    }

    /// The `error_message` of the heartbeat response.
    #[must_use]
    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::InvalidTopology(message) | Self::InvalidTopic(message) => Some(message.clone()),
            Self::Internal(_) => None,
        }
    }
}

/// Why [`configure`] stopped: a status that Kafka catches in
/// `configureTopics`, or an error that leaves the heartbeat.
enum Stop {
    Status(i8, String),
    Error(ConfigureTopicsError),
}

impl From<ConfigureTopicsError> for Stop {
    fn from(error: ConfigureTopicsError) -> Self { Self::Error(error) }
}

/// Kafka's `InternalTopicManager.configureTopics`.
///
/// # Errors
///
/// Returns the error that Kafka throws out of the heartbeat for a topology
/// that cannot be configured.
pub fn configure_topics(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<ConfiguredTopology, ConfigureTopicsError> {
    let copartition_groups = topology
        .subtopologies
        .iter()
        .map(copartition_groups)
        .collect::<Result<Vec<_>, _>>()?;
    match configure(topology, image, &copartition_groups) {
        Ok(Configured {
            subtopologies,
            internal_topics_to_create,
        }) => {
            let status = (!internal_topics_to_create.is_empty()).then(|| TopologyStatus {
                code: status::MISSING_INTERNAL_TOPICS,
                detail: format!(
                    "Internal topics are missing: {}",
                    summarize_topics(internal_topics_to_create.keys())
                ),
            });
            Ok(ConfiguredTopology {
                topology_epoch: topology.epoch,
                subtopologies: Some(subtopologies),
                internal_topics_to_create,
                status,
            })
        }
        Err(Stop::Status(code, detail)) => Ok(ConfiguredTopology {
            topology_epoch: topology.epoch,
            subtopologies: None,
            internal_topics_to_create: BTreeMap::new(),
            status: Some(TopologyStatus { code, detail }),
        }),
        Err(Stop::Error(error)) => Err(error),
    }
}

/// The subtopologies of a topology, sized, and the internal topics that the
/// image does not hold.
struct Configured {
    subtopologies: BTreeMap<String, ConfiguredSubtopology>,
    internal_topics_to_create: BTreeMap<String, ConfiguredInternalTopic>,
}

fn configure(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    copartition_groups: &[Vec<BTreeSet<String>>],
) -> Result<Configured, Stop> {
    throw_on_missing_source_topics(topology, image)?;
    let decided = decide_partition_counts(topology, image, copartition_groups)?;
    let mut subtopologies = BTreeMap::new();
    for subtopology in &topology.subtopologies {
        let configured = from_persisted_subtopology(subtopology, image, &decided)?;
        if subtopologies
            .insert(subtopology.subtopology_id.clone(), configured)
            .is_some()
        {
            return Err(ConfigureTopicsError::Internal(format!(
                "Duplicate key {}",
                subtopology.subtopology_id
            ))
            .into());
        }
    }
    let internal_topics_to_create = missing_internal_topics(&subtopologies, topology, image)?;
    Ok(Configured {
        subtopologies,
        internal_topics_to_create,
    })
}

/// Kafka's `throwOnMissingSourceTopics`.
fn throw_on_missing_source_topics(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<(), Stop> {
    let missing: BTreeSet<&str> = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.source_topics.iter())
        .filter(|topic| image.topic(topic).is_none())
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Stop::Status(
            status::MISSING_SOURCE_TOPICS,
            format!("Source topics {} are missing.", summarize_topics(missing)),
        ))
    }
}

/// Kafka's `decidePartitionCounts`: repartition topics, then copartition
/// coercion, then changelog topics.
fn decide_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    copartition_groups: &[Vec<BTreeSet<String>>],
) -> Result<BTreeMap<String, i32>, Stop> {
    let mut decided = repartition_topic_partition_counts(topology, image)?;

    let (fixed, flexible) = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .partition::<Vec<&StoredTopicInfo>, _>(|topic| topic.partitions != 0);
    let fixed: BTreeSet<&str> = fixed.into_iter().map(|topic| topic.name.as_str()).collect();
    let flexible: BTreeSet<&str> = flexible
        .into_iter()
        .map(|topic| topic.name.as_str())
        .collect();
    for group in copartition_groups.iter().flatten() {
        let coerced = enforce_copartitioning(group, &fixed, &flexible, image, &decided)?;
        decided.extend(coerced);
    }

    let changelogs = changelog_topic_partition_counts(topology, image, &decided)?;
    decided.extend(changelogs);
    Ok(decided)
}

/// Kafka's `getPartitionCount`: the partition count in the image, else the
/// decided count of an internal topic.
fn partition_count(
    image: &MetadataImage,
    topic: &str,
    decided: &BTreeMap<String, i32>,
) -> Option<i32> {
    if image.topic(topic).is_some() {
        Some(image.topic_partition_count(topic))
    } else {
        decided.get(topic).copied()
    }
}

/// Kafka's `RepartitionTopics.setup`.
fn repartition_topic_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<BTreeMap<String, i32>, Stop> {
    let no_decisions = BTreeMap::new();
    let mut counts: BTreeMap<String, i32> = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .filter(|topic| topic.partitions != 0)
        .map(|topic| (topic.name.clone(), topic.partitions))
        .collect();
    loop {
        let mut partition_count_needed = false;
        let mut progress_made = false;
        for subtopology in &topology.subtopologies {
            for sink in &subtopology.repartition_sink_topics {
                if counts.contains_key(sink) {
                    continue;
                }
                // The largest count of the repartition source topics known so
                // far and of the external source topics.
                let candidate = subtopology
                    .repartition_source_topics
                    .iter()
                    .filter_map(|topic| counts.get(&topic.name).copied())
                    .chain(
                        subtopology
                            .source_topics
                            .iter()
                            .filter_map(|topic| partition_count(image, topic, &no_decisions)),
                    )
                    .max();
                if let Some(partitions) = candidate {
                    counts.insert(sink.clone(), partitions);
                    progress_made = true;
                } else {
                    partition_count_needed = true;
                }
            }
        }
        if !progress_made && partition_count_needed {
            return Err(ConfigureTopicsError::InvalidTopology(
                "Failed to compute number of partitions for all repartition topics. There may be \
                 loops in the topology that cannot be resolved."
                    .into(),
            )
            .into());
        }
        if !partition_count_needed {
            break;
        }
    }
    let never_written = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .any(|topic| !counts.contains_key(&topic.name));
    if never_written {
        return Err(ConfigureTopicsError::InvalidTopology(
            "Failed to compute number of partitions for all repartition topics, because a \
             repartition source topic is never used as a sink topic."
                .into(),
        )
        .into());
    }
    Ok(counts)
}

/// Kafka's `CopartitionedTopicsEnforcer.enforce`. `fixed` and `flexible` are
/// the repartition source topics of the whole topology with and without an
/// explicit partition count.
fn enforce_copartitioning(
    group: &BTreeSet<String>,
    fixed: &BTreeSet<&str>,
    flexible: &BTreeSet<&str>,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<BTreeMap<String, i32>, Stop> {
    if group.is_empty() {
        return Ok(BTreeMap::new());
    }
    let count_of = |topic: &str| {
        partition_count(image, topic, decided).ok_or_else(|| {
            Stop::Error(ConfigureTopicsError::Internal(format!(
                "Number of partitions is not set for topic: {topic}"
            )))
        })
    };
    let mut repartition_counts = BTreeMap::new();
    let mut external_counts = BTreeMap::new();
    for topic in group {
        let count = count_of(topic)?;
        if fixed.contains(topic.as_str()) || flexible.contains(topic.as_str()) {
            repartition_counts.insert(topic.clone(), count);
        } else {
            external_counts.insert(topic.clone(), count);
        }
    }

    let partitions = if external_counts.is_empty() {
        if fixed.is_empty() {
            let max = repartition_counts
                .values()
                .copied()
                .max()
                .unwrap_or(0)
                .max(0);
            if max == 0 {
                return Err(ConfigureTopicsError::InvalidTopology(format!(
                    "All topics in the copartition group had undefined partition number: [{}]",
                    repartition_counts
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .into());
            }
            max
        } else {
            // Kafka compares every fixed repartition topic of the topology,
            // not only the ones in this group.
            let mut first = None;
            for topic in fixed {
                let count = count_of(topic)?;
                match first {
                    None => first = Some(count),
                    Some(expected) if expected != count => {
                        return Err(Stop::Status(
                            status::INCORRECTLY_PARTITIONED_TOPICS,
                            format!(
                                "Following topics do not have the same number of partitions: \
                                 [{}]",
                                java_map_string(&repartition_counts)
                            ),
                        ));
                    }
                    Some(_) => {}
                }
            }
            first.unwrap_or_default()
        }
    } else {
        let mut counts = external_counts.values().copied();
        let first = counts.next().unwrap_or_default();
        if counts.any(|count| count != first) {
            return Err(Stop::Status(
                status::INCORRECTLY_PARTITIONED_TOPICS,
                format!(
                    "Following topics do not have the same number of partitions: [{}]",
                    java_map_string(&external_counts)
                ),
            ));
        }
        first
    };

    let mut coerced = BTreeMap::new();
    for (topic, count) in repartition_counts {
        if fixed.contains(topic.as_str()) && count != partitions {
            return Err(Stop::Status(
                status::INCORRECTLY_PARTITIONED_TOPICS,
                format!(
                    "Number of partitions [{count}] of repartition topic [{topic}] doesn't match \
                     number of partitions [{partitions}] of the source topic."
                ),
            ));
        }
        coerced.insert(topic, partitions);
    }
    Ok(coerced)
}

/// Kafka's `ChangelogTopics.setup`.
fn changelog_topic_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<BTreeMap<String, i32>, Stop> {
    let mut counts = BTreeMap::new();
    for subtopology in &topology.subtopologies {
        let mut max = None;
        for topic in input_topics(subtopology) {
            let count = partition_count(image, topic, decided).ok_or_else(|| {
                ConfigureTopicsError::Internal(format!(
                    "No partition count for source topic {topic}"
                ))
            })?;
            max = Some(max.map_or(count, |max: i32| max.max(count)));
        }
        let Some(max) = max else {
            return Err(ConfigureTopicsError::InvalidTopology(format!(
                "No source topics found for subtopology {}",
                subtopology.subtopology_id
            ))
            .into());
        };
        for topic in &subtopology.state_changelog_topics {
            counts.insert(topic.name.clone(), max);
        }
    }
    Ok(counts)
}

/// The source topics and then the repartition source topics of a
/// subtopology.
pub(super) fn input_topics(subtopology: &StoredSubtopology) -> impl Iterator<Item = &str> {
    subtopology.source_topics.iter().map(String::as_str).chain(
        subtopology
            .repartition_source_topics
            .iter()
            .map(|topic| topic.name.as_str()),
    )
}

/// Kafka's `fromPersistedSubtopology`.
fn from_persisted_subtopology(
    subtopology: &StoredSubtopology,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<ConfiguredSubtopology, ConfigureTopicsError> {
    let mut number_of_tasks = None;
    for topic in input_topics(subtopology) {
        let count = partition_count(image, topic, decided).ok_or_else(|| {
            ConfigureTopicsError::Internal(format!(
                "Number of partitions must be set for topic {topic}"
            ))
        })?;
        number_of_tasks = Some(number_of_tasks.map_or(count, |max: i32| max.max(count)));
    }
    let number_of_tasks = number_of_tasks.ok_or_else(|| {
        ConfigureTopicsError::Internal("Subtopology does not contain any source topics".into())
    })?;
    let internal = |topics: &[StoredTopicInfo]| {
        topics
            .iter()
            .map(|topic| {
                from_persisted_topic_info(topic, decided).map(|topic| (topic.name.clone(), topic))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    };
    Ok(ConfiguredSubtopology {
        number_of_tasks,
        source_topics: subtopology.source_topics.iter().cloned().collect(),
        repartition_source_topics: internal(&subtopology.repartition_source_topics)?,
        repartition_sink_topics: subtopology
            .repartition_sink_topics
            .iter()
            .cloned()
            .collect(),
        state_changelog_topics: internal(&subtopology.state_changelog_topics)?,
    })
}

/// Kafka's `fromPersistedTopicInfo` and the `ConfiguredInternalTopic` checks.
fn from_persisted_topic_info(
    topic: &StoredTopicInfo,
    decided: &BTreeMap<String, i32>,
) -> Result<ConfiguredInternalTopic, ConfigureTopicsError> {
    let partitions = if topic.partitions == 0 {
        *decided.get(&topic.name).ok_or_else(|| {
            ConfigureTopicsError::Internal(format!(
                "Number of partitions must be set for topic {}",
                topic.name
            ))
        })?
    } else {
        topic.partitions
    };
    // Java evaluates the config map before the `ConfiguredInternalTopic`
    // constructor checks the name and the partition count.
    let mut configs = BTreeMap::new();
    for (key, value) in &topic.topic_configs {
        if configs.insert(key.clone(), value.clone()).is_some() {
            return Err(ConfigureTopicsError::Internal(format!(
                "Duplicate key {key}"
            )));
        }
    }
    validate_topic_name(&topic.name)
        .map_err(|invalid| ConfigureTopicsError::InvalidTopic(invalid.to_string()))?;
    if partitions < 1 {
        return Err(ConfigureTopicsError::Internal(
            "Number of partitions must be at least 1.".into(),
        ));
    }
    Ok(ConfiguredInternalTopic {
        name: topic.name.clone(),
        partitions,
        replication_factor: (topic.replication_factor != 0).then_some(topic.replication_factor),
        configs,
    })
}

/// Kafka's `copartitionGroupsFromPersistedSubtopology`: each group as the set
/// of topic names that its indices select.
fn copartition_groups(
    subtopology: &StoredSubtopology,
) -> Result<Vec<BTreeSet<String>>, ConfigureTopicsError> {
    let out_of_range = |index: i16, length: usize| {
        ConfigureTopicsError::Internal(format!("Index {index} out of bounds for length {length}"))
    };
    subtopology
        .copartition_groups
        .iter()
        .map(|group| {
            let sources = group.source_topics.iter().map(|&index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|position| subtopology.source_topics.get(position))
                    .cloned()
                    .ok_or_else(|| out_of_range(index, subtopology.source_topics.len()))
            });
            let repartition_sources = group.repartition_source_topics.iter().map(|&index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|position| subtopology.repartition_source_topics.get(position))
                    .map(|topic| topic.name.clone())
                    .ok_or_else(|| out_of_range(index, subtopology.repartition_source_topics.len()))
            });
            sources.chain(repartition_sources).collect()
        })
        .collect()
}

/// Kafka's `missingInternalTopics`.
fn missing_internal_topics(
    subtopologies: &BTreeMap<String, ConfiguredSubtopology>,
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<BTreeMap<String, ConfiguredInternalTopic>, Stop> {
    let mut to_create: BTreeMap<String, ConfiguredInternalTopic> = subtopologies
        .values()
        .flat_map(|subtopology| {
            subtopology
                .repartition_source_topics
                .values()
                .chain(subtopology.state_changelog_topics.values())
        })
        .map(|topic| (topic.name.clone(), topic.clone()))
        .collect();
    for topic in required_topics(topology) {
        if image.topic(topic).is_none() {
            continue;
        }
        if let Some(expected) = to_create.remove(topic) {
            let found = image.topic_partition_count(topic);
            if found != expected.partitions {
                return Err(Stop::Status(
                    status::INCORRECTLY_PARTITIONED_TOPICS,
                    format!(
                        "Existing topic {topic} has different number of partitions: expected {}, \
                         found {found}",
                        expected.partitions
                    ),
                ));
            }
        }
    }
    Ok(to_create)
}

/// Kafka's `InternalTopicManager.summarizeTopics`: at most three names, then
/// the count of the others.
fn summarize_topics<S: AsRef<str>>(topics: impl IntoIterator<Item = S>) -> String {
    let topics: Vec<S> = topics.into_iter().collect();
    if topics.is_empty() {
        return "<none>".into();
    }
    let shown = topics
        .iter()
        .take(3)
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ");
    if topics.len() > 3 {
        format!("{shown} and {} additional topics", topics.len() - 3)
    } else {
        shown
    }
}

/// Java's `TreeMap.toString`: `{a=1, b=2}`.
fn java_map_string(map: &BTreeMap<String, i32>) -> String {
    let entries = map
        .iter()
        .map(|(topic, count)| format!("{topic}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{entries}}}")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::streams::{
            persistence::StoredCopartitionGroup, topology::test_support::image_with,
        },
        assert2::check,
    };

    /// A row: its label, the topology and image to configure, and the whole
    /// configured topology or error it gives.
    #[test]
    fn configure_topics_error_codes_and_messages() {
        let rows = [
            (
                ConfigureTopicsError::InvalidTopology("t".into()),
                codes::STREAMS_INVALID_TOPOLOGY,
                Some("t".to_owned()),
            ),
            (
                ConfigureTopicsError::InvalidTopic("n".into()),
                codes::INVALID_TOPIC_EXCEPTION,
                Some("n".to_owned()),
            ),
            (
                ConfigureTopicsError::Internal("i".into()),
                codes::UNKNOWN_SERVER_ERROR,
                None,
            ),
        ];
        for (error, code, message) in rows {
            check!((error.error_code(), error.error_message()) == (code, message));
        }
    }
    use {
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        serde::Deserialize,
        serde_json::{Value, json},
    };
    #[derive(Default)]
    struct ConfiguredTopics {
        topology: Option<StreamsGroupTopologyValue>,
        image: Option<MetadataImage>,
        outcome: Option<Result<ConfiguredTopology, ConfigureTopicsError>>,
    }
    #[rstest::fixture]
    fn configured_topics() -> ConfiguredTopics {
        // Start this scenario with fresh inputs and observations.
        ConfiguredTopics::default()
    }

    #[derive(Deserialize)]
    struct TopologyInput {
        epoch: i32,
        subtopologies: Vec<SubtopologyInput>,
    }
    #[derive(Deserialize)]
    struct InternalTopicInput {
        name: String,
        partitions: i32,
        replication_factor: i16,
        topic_configs: Vec<(String, String)>,
    }
    #[derive(Deserialize)]
    struct CopartitionInput {
        source_topics: Vec<i16>,
        source_topic_regex: Vec<i16>,
        repartition_source_topics: Vec<i16>,
    }
    #[derive(Deserialize)]
    struct SubtopologyInput {
        subtopology_id: String,
        source_topics: Vec<String>,
        source_topic_regex: Vec<String>,
        repartition_sink_topics: Vec<String>,
        state_changelog_topics: Vec<InternalTopicInput>,
        repartition_source_topics: Vec<InternalTopicInput>,
        copartition_groups: Vec<CopartitionInput>,
    }
    impl From<InternalTopicInput> for StoredTopicInfo {
        fn from(value: InternalTopicInput) -> Self {
            Self {
                name: value.name,
                partitions: value.partitions,
                replication_factor: value.replication_factor,
                topic_configs: value.topic_configs,
            }
        }
    }
    impl From<TopologyInput> for StreamsGroupTopologyValue {
        fn from(value: TopologyInput) -> Self {
            Self {
                epoch: value.epoch,
                subtopologies: value
                    .subtopologies
                    .into_iter()
                    .map(|sub| StoredSubtopology {
                        subtopology_id: sub.subtopology_id,
                        source_topics: sub.source_topics,
                        source_topic_regex: sub.source_topic_regex,
                        repartition_sink_topics: sub.repartition_sink_topics,
                        state_changelog_topics: sub
                            .state_changelog_topics
                            .into_iter()
                            .map(StoredTopicInfo::from)
                            .collect(),
                        repartition_source_topics: sub
                            .repartition_source_topics
                            .into_iter()
                            .map(StoredTopicInfo::from)
                            .collect(),
                        copartition_groups: sub
                            .copartition_groups
                            .into_iter()
                            .map(|group| StoredCopartitionGroup {
                                source_topics: group.source_topics,
                                source_topic_regex: group.source_topic_regex,
                                repartition_source_topics: group.repartition_source_topics,
                            })
                            .collect(),
                    })
                    .collect(),
            }
        }
    }
    #[derive(DataTableRow)]
    struct SourceTopicInput {
        name: String,
        id: u8,
        partitions: i32,
    }
    #[given("the Streams application publishes this complete topology")]
    fn published_application_topology(configured_topics: &mut ConfiguredTopics, docstring: String) {
        configured_topics.topology = Some(StreamsGroupTopologyValue::from(
            serde_json::from_str::<TopologyInput>(&docstring).expect("complete topology input"),
        ));
    }
    #[given("the metadata image contains these topic partition counts")]
    fn configured_source_topics(
        configured_topics: &mut ConfiguredTopics,
        #[datatable] rows: Rows<SourceTopicInput>,
    ) {
        let rows = rows.into_iter().collect::<Vec<_>>();
        let topics = rows
            .iter()
            .map(|row| (row.name.as_str(), row.id, row.partitions))
            .collect::<Vec<_>>();
        configured_topics.image = Some(image_with(&topics));
    }
    #[when("the internal topic manager configures the topology against the metadata image")]
    fn configure_internal_topics(configured_topics: &mut ConfiguredTopics) {
        configured_topics.outcome = Some(configure_topics(
            configured_topics
                .topology
                .as_ref()
                .expect("application topology given"),
            configured_topics
                .image
                .as_ref()
                .expect("metadata topics given"),
        ));
    }
    fn configured_topic_snapshot(info: &ConfiguredInternalTopic) -> Value {
        let ConfiguredInternalTopic {
            name,
            partitions,
            replication_factor,
            configs,
        } = info;
        json!({"name":name,"partitions":partitions,"replication_factor":replication_factor,"configs":configs})
    }
    fn configured_topic_snapshots(topics: &BTreeMap<String, ConfiguredInternalTopic>) -> Value {
        json!(
            topics
                .iter()
                .map(|(name, topic)| (name.clone(), configured_topic_snapshot(topic)))
                .collect::<BTreeMap<_, _>>()
        )
    }
    fn configured_subtopology_snapshot(subtopology: &ConfiguredSubtopology) -> Value {
        let ConfiguredSubtopology {
            number_of_tasks,
            source_topics,
            repartition_source_topics,
            repartition_sink_topics,
            state_changelog_topics,
        } = subtopology;
        json!({
            "number_of_tasks":number_of_tasks,
            "source_topics":source_topics,
            "repartition_source_topics":configured_topic_snapshots(repartition_source_topics),
            "repartition_sink_topics":repartition_sink_topics,
            "state_changelog_topics":configured_topic_snapshots(state_changelog_topics),
        })
    }
    fn configured_outcome_snapshot(
        result: &Result<ConfiguredTopology, ConfigureTopicsError>,
    ) -> Value {
        match result {
            Ok(topology) => {
                let ConfiguredTopology {
                    topology_epoch,
                    subtopologies,
                    internal_topics_to_create,
                    status,
                } = topology;
                let subtopologies = subtopologies.as_ref().map(|subs| {
                    subs.iter()
                        .map(|(id, sub)| (id.clone(), configured_subtopology_snapshot(sub)))
                        .collect::<BTreeMap<_, _>>()
                });
                let status = status
                    .as_ref()
                    .map(|TopologyStatus { code, detail }| json!({"code":code,"detail":detail}));
                json!({
                    "topology_epoch":topology_epoch,
                    "subtopologies":subtopologies,
                    "internal_topics_to_create":configured_topic_snapshots(internal_topics_to_create),
                    "status":status,
                })
            }
            Err(error) => {
                let (kind, message) = match error {
                    ConfigureTopicsError::InvalidTopology(message) => ("invalid-topology", message),
                    ConfigureTopicsError::InvalidTopic(message) => ("invalid-topic", message),
                    ConfigureTopicsError::Internal(message) => ("internal", message),
                };
                json!({"error_kind":kind,"message":message})
            }
        }
    }
    #[then("the complete configured topology or exact configuration error is")]
    fn expected_configured_topology(configured_topics: &ConfiguredTopics, docstring: String) {
        let expected = serde_json::from_str::<Value>(&docstring)
            .expect("complete configured topology expectation");
        check!(
            configured_outcome_snapshot(
                configured_topics
                    .outcome
                    .as_ref()
                    .expect("topology configured")
            ) == expected
        );
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/streams/topology/configured.feature",fixtures=[configured_topics:ConfiguredTopics]);
}
