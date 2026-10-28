//! The rules that decide whether a group's dead-letter topic can take a write:
//! Kafka's `ShareGroupDLQValidator.validateDlqTopicConfig` and
//! `ShareGroupDLQStateManager.ProduceRequestHandler.validateDlqTopic`.
//!
//! Kafka checks the group's settings, the topic and the two cluster settings
//! against the cluster metadata when a write starts, so an operator's change
//! takes effect for the next write. So does this module: it reads them out of
//! the metadata image each time.

use {
    super::DlqError,
    crate::{
        config_keys::ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
        share_partition::group_settings::{KEY_DLQ_COPY_RECORD_ENABLE, KEY_DLQ_TOPIC_NAME},
    },
    krabka_metadata::{MetadataImage, NodeId},
};

/// Kafka's `GroupCoordinatorConfig.ERRORS_DEADLETTERQUEUE_AUTO_CREATE_TOPICS_ENABLE_CONFIG`.
const KEY_AUTO_CREATE: &str = "errors.deadletterqueue.auto.create.topics.enable";
/// Kafka's `GroupCoordinatorConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_PREFIX_CONFIG`.
const KEY_TOPIC_PREFIX: &str = "errors.deadletterqueue.topic.name.prefix";
/// The default of [`KEY_TOPIC_PREFIX`].
const DEFAULT_TOPIC_PREFIX: &str = "dlq.";

/// The two cluster-wide settings of the dead-letter queue. Both are dynamic
/// broker configs in Kafka (`GroupCoordinatorConfig.RECONFIGURABLE_CONFIGS`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClusterSettings {
    /// Whether a missing dead-letter topic is created: Kafka's
    /// `errors.deadletterqueue.auto.create.topics.enable`, default `false`.
    pub(super) auto_create: bool,
    /// The prefix that a dead-letter topic name must have, or `""` for no
    /// rule: Kafka's `errors.deadletterqueue.topic.name.prefix`, default
    /// `dlq.`.
    pub(super) topic_prefix: String,
}

impl ClusterSettings {
    /// The settings of `node`: its own value where it has one, else the
    /// cluster-wide one, else Kafka's default.
    pub(super) fn resolve(image: &MetadataImage, node: NodeId) -> Self {
        let value = |key: &str| {
            image
                .broker_config(node)
                .and_then(|configs| configs.get(key))
                .or_else(|| image.default_broker_config()?.get(key))
        };
        Self {
            auto_create: value(KEY_AUTO_CREATE).is_some_and(|enabled| is_true(enabled)),
            topic_prefix: value(KEY_TOPIC_PREFIX).map_or_else(
                || DEFAULT_TOPIC_PREFIX.to_owned(),
                |prefix| prefix.trim().to_owned(),
            ),
        }
    }
}

/// A boolean config, as Kafka's `ConfigDef` reads it: `true` in any case.
fn is_true(value: &str) -> bool { value.trim().eq_ignore_ascii_case("true") }

/// The dead-letter settings of one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GroupSettings {
    /// `errors.deadletterqueue.topic.name`: never empty.
    pub(super) topic: String,
    /// `errors.deadletterqueue.copy.record.enable`: whether the dead-letter
    /// record carries the key and the value of the source record.
    pub(super) copy_record: bool,
}

impl GroupSettings {
    /// The settings of `group`, or `None` when it names no dead-letter topic.
    pub(super) fn resolve(image: &MetadataImage, group: &str) -> Option<Self> {
        let configs = image.group_config(group)?;
        let topic = configs.get(KEY_DLQ_TOPIC_NAME)?.trim();
        (!topic.is_empty()).then(|| Self {
            topic: topic.to_owned(),
            copy_record: configs
                .get(KEY_DLQ_COPY_RECORD_ENABLE)
                .is_some_and(|enabled| is_true(enabled)),
        })
    }
}

/// Whether the dead-letter topic is there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TopicState {
    Exists,
    /// It is not there, and the cluster creates it.
    Missing,
}

/// The settings of a group whose dead-letter topic passed validation, and the
/// state of the topic.
pub(super) struct ValidatedGroup {
    pub(super) settings: GroupSettings,
    pub(super) topic_state: TopicState,
}

/// Checks the topic of `group` against the cluster's rules, in Kafka's order:
///
/// 1. the group names a topic;
/// 2. the name does not start with `__`, the prefix of the internal topics;
/// 3. a topic that exists has `errors.deadletterqueue.group.enable=true`;
/// 4. the name starts with the cluster's prefix, unless that is empty;
/// 5. a topic that does not exist can be created.
///
/// It returns the settings of the group with the state of the topic.
///
/// # Errors
///
/// [`DlqError::Config`], with Kafka's message for the rule that failed.
pub(super) fn validate(
    image: &MetadataImage,
    group: &str,
    settings: Option<GroupSettings>,
    cluster: &ClusterSettings,
) -> Result<ValidatedGroup, DlqError> {
    let Some(settings) = settings else {
        return Err(DlqError::Config(format!(
            "Configured DLQ topic name in share group: {group} is empty."
        )));
    };
    let topic = settings.topic.as_str();
    if topic.starts_with("__") {
        return Err(DlqError::Config(format!(
            "Configured DLQ topic name in share group: {group} cannot start with __, topic: {topic}."
        )));
    }
    let exists = image.topic(topic).is_some();
    if exists
        && !image
            .topic_config(topic)
            .and_then(|configs| configs.get(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE))
            .is_some_and(|enabled| is_true(enabled))
    {
        return Err(DlqError::Config(format!(
            "DLQ is not enabled on configured DLQ topic for share group: {group}, topic: {topic}"
        )));
    }
    let prefix = cluster.topic_prefix.as_str();
    if !prefix.is_empty() && !topic.starts_with(prefix) {
        return Err(DlqError::Config(format!(
            "Configured DLQ topic name does not comply with the DLQ topic prefix in share group: \
             {group}, topic: {topic}, prefix: {prefix}"
        )));
    }
    if exists {
        return Ok(ValidatedGroup {
            settings,
            topic_state: TopicState::Exists,
        });
    }
    if !cluster.auto_create {
        return Err(DlqError::Config(format!(
            "DLQ topic does not exist and auto create is disabled on cluster for share group: \
             {group}, topic: {topic}."
        )));
    }
    Ok(ValidatedGroup {
        settings,
        topic_state: TopicState::Missing,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::test_support::{ConfigEntry, config_map as configs, entry},
        assert2::assert,
        krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, GroupConfigRecord, MetadataRecord,
            TopicConfigRecord, TopicRecord,
        },
    };

    fn broker_config(node_id: NodeId, name: &str, value: &str) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.to_owned(),
            config_value: Some(value.to_owned()),
        })
    }

    /// An image with `group` configured as `group_configs`, and the topic
    /// `topic` if `topic_configs` is `Some`.
    fn image(
        group_configs: &[ConfigEntry<'_>],
        topic_configs: Option<&[ConfigEntry<'_>]>,
    ) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "g".into(),
            configs: configs(group_configs),
        }));
        if let Some(topic_configs) = topic_configs {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: "dlq.g".into(),
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                partitions: 1,
                replication_factor: 1,
            }));
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "dlq.g".into(),
                overrides: configs(topic_configs),
            }));
        }
        image
    }

    const NODE: NodeId = NodeId(1);

    fn outcome(image: &MetadataImage) -> Result<TopicState, DlqError> {
        validate(
            image,
            "g",
            GroupSettings::resolve(image, "g"),
            &ClusterSettings::resolve(image, NODE),
        )
        .map(|validated| validated.topic_state)
    }

    fn config_error(message: &str) -> Result<TopicState, DlqError> {
        Err(DlqError::Config(message.to_owned()))
    }

    #[derive(Default)]
    struct GroupDeadLetterTopic {
        image: Option<MetadataImage>,
        validated: Option<Result<TopicState, DlqError>>,
    }

    #[rstest::fixture]
    fn group_dead_letter_topic() -> GroupDeadLetterTopic {
        // Each share group validates against an independently configured cluster.
        GroupDeadLetterTopic::default()
    }

    #[rstest_bdd_macros::given("share group g has no configured dead-letter topic")]
    fn unconfigured_group_dead_letter_topic(group_dead_letter_topic: &mut GroupDeadLetterTopic) {
        group_dead_letter_topic.image = Some(image(&[], None));
    }

    #[rstest_bdd_macros::given("share group g names dead-letter topic {topic}")]
    fn named_group_dead_letter_topic(
        group_dead_letter_topic: &mut GroupDeadLetterTopic,
        topic: String,
    ) {
        group_dead_letter_topic
            .image
            .as_mut()
            .expect("share group was given")
            .apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
                group_id: "g".to_owned(),
                configs: configs(&[entry(KEY_DLQ_TOPIC_NAME, &topic)]),
            }));
    }

    #[rstest_bdd_macros::given("topic dlq.g exists with dead-letter opt-in {flag}")]
    fn existing_group_dead_letter_topic(
        group_dead_letter_topic: &mut GroupDeadLetterTopic,
        flag: String,
    ) {
        use uuid::Uuid;
        let image = group_dead_letter_topic
            .image
            .as_mut()
            .expect("share group was given");
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "dlq.g".to_owned(),
            topic_id: Uuid::from_bytes([9; 16]),
            partitions: 1,
            replication_factor: 1,
        }));
        let overrides = match flag.as_str() {
            "true" => configs(&[entry(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, "true")]),
            "false" => configs(&[entry(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, "false")]),
            "unset" => configs(&[]),
            _ => panic!("unknown dead-letter opt-in setting {flag}"),
        };
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "dlq.g".to_owned(),
            overrides,
        }));
    }

    #[rstest_bdd_macros::given("the cluster permits automatic dead-letter topic creation")]
    fn automatically_created_group_dead_letter_topic(
        group_dead_letter_topic: &mut GroupDeadLetterTopic,
    ) {
        group_dead_letter_topic
            .image
            .as_mut()
            .expect("share group was given")
            .apply(&broker_config(
                DEFAULT_BROKER_CONFIG_NODE_ID,
                KEY_AUTO_CREATE,
                "true",
            ));
    }

    #[rstest_bdd_macros::when("the share partition validates group g's dead-letter topic")]
    fn validate_group_dead_letter_topic(group_dead_letter_topic: &mut GroupDeadLetterTopic) {
        group_dead_letter_topic.validated = Some(outcome(
            group_dead_letter_topic
                .image
                .as_ref()
                .expect("share group was given"),
        ));
    }

    #[rstest_bdd_macros::then("dead-letter configuration fails with {message}")]
    fn refused_group_dead_letter_topic(
        group_dead_letter_topic: &GroupDeadLetterTopic,
        message: String,
    ) {
        assert!(group_dead_letter_topic.validated.as_ref() == Some(&config_error(&message)));
    }

    #[rstest_bdd_macros::then("dead-letter validation accepts topic state {state}")]
    fn accepted_group_dead_letter_topic(
        group_dead_letter_topic: &GroupDeadLetterTopic,
        state: String,
    ) {
        let expected = match state.as_str() {
            "exists" => TopicState::Exists,
            "missing and creatable" => TopicState::Missing,
            _ => panic!("unknown expected DLQ topic state {state}"),
        };
        assert!(group_dead_letter_topic.validated.as_ref() == Some(&Ok(expected)));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/share_partition/dlq/validate.feature",
        fixtures = [group_dead_letter_topic: GroupDeadLetterTopic]
    );

    /// The prefix is a dynamic cluster config: an empty one lifts the rule, and
    /// a broker's own value beats the cluster default.
    #[test]
    fn the_prefix_follows_the_dynamic_broker_config() {
        let mut image = image(&[entry(KEY_DLQ_TOPIC_NAME, "orders.dead")], None);
        let mut settings = |records: &[MetadataRecord]| {
            for record in records {
                image.apply(record);
            }
            ClusterSettings::resolve(&image, NODE)
        };
        let default = settings(&[]);
        let cluster_empty = settings(&[broker_config(
            DEFAULT_BROKER_CONFIG_NODE_ID,
            KEY_TOPIC_PREFIX,
            "",
        )]);
        let broker_own = settings(&[broker_config(NODE, KEY_TOPIC_PREFIX, "dead.")]);

        assert!(
            [default, cluster_empty, broker_own]
                == [
                    ClusterSettings {
                        auto_create: false,
                        topic_prefix: "dlq.".into()
                    },
                    ClusterSettings {
                        auto_create: false,
                        topic_prefix: String::new()
                    },
                    ClusterSettings {
                        auto_create: false,
                        topic_prefix: "dead.".into()
                    },
                ]
        );
    }

    #[test]
    fn a_group_that_names_no_topic_has_no_settings() {
        struct Case {
            group_configs: Vec<ConfigEntry<'static>>,
            settings: Option<GroupSettings>,
        }
        let cases: [Case; 4] = [
            Case {
                group_configs: vec![],
                settings: None,
            },
            Case {
                group_configs: vec![entry(KEY_DLQ_TOPIC_NAME, "  ")],
                settings: None,
            },
            Case {
                group_configs: vec![entry(KEY_DLQ_TOPIC_NAME, " dlq.g ")],
                settings: Some(GroupSettings {
                    topic: "dlq.g".into(),
                    copy_record: false,
                }),
            },
            Case {
                group_configs: vec![
                    entry(KEY_DLQ_TOPIC_NAME, "dlq.g"),
                    entry(KEY_DLQ_COPY_RECORD_ENABLE, "TRUE"),
                ],
                settings: Some(GroupSettings {
                    topic: "dlq.g".into(),
                    copy_record: true,
                }),
            },
        ];

        let actual: Vec<_> = cases
            .iter()
            .map(|case| GroupSettings::resolve(&image(&case.group_configs, None), "g"))
            .collect();

        assert!(
            actual
                == cases
                    .iter()
                    .map(|case| case.settings.clone())
                    .collect::<Vec<_>>()
        );
    }
}
