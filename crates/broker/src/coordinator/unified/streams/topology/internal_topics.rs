//! Internal-topic specs and their materialization.
//!
//! A topology needs a repartition topic for every `repartition_source_topics`
//! entry and a changelog topic for every `state_changelog_topics` entry. This
//! module turns the internal topics that the configured topology still misses
//! into specs, and it is the one place in topology handling that writes
//! metadata records through the controller.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord};
use krabka_raft::RaftError;
use uuid::Uuid;

use super::configured::ConfiguredTopology;
use crate::{error::BrokerError, metadata_source::MetadataSource};

/// A fully-resolved internal topic that the coordinator must materialize.
///
/// The topic is a repartition topic or a changelog topic. The spec holds its
/// partition count, its replication factor, and its config overrides. A
/// replication factor of 0 means "use the cluster default".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalTopicSpec {
    pub name: String,
    pub partitions: i32,
    /// Replication factor that the client requested.
    ///
    /// A value of `0` uses the configured cluster default, with a cap at the
    /// number of available brokers.
    pub replication_factor: i16,
    pub configs: BTreeMap<String, String>,
}

/// The specs of the internal topics that `configured` must create, in name
/// order.
///
/// A changelog topic gets `cleanup.policy=compact`, and a repartition topic
/// gets `cleanup.policy=delete`, unless the topology sets the policy.
#[must_use]
pub fn internal_topic_specs(configured: &ConfiguredTopology) -> Vec<InternalTopicSpec> {
    let changelogs: BTreeSet<&str> = configured
        .subtopologies
        .iter()
        .flatten()
        .flat_map(|(_, subtopology)| subtopology.state_changelog_topics.keys())
        .map(String::as_str)
        .collect();
    configured
        .internal_topics_to_create
        .values()
        .map(|topic| {
            let cleanup_policy = if changelogs.contains(topic.name.as_str()) {
                "compact"
            } else {
                "delete"
            };
            let mut configs = topic.configs.clone();
            configs
                .entry("cleanup.policy".to_string())
                .or_insert_with(|| cleanup_policy.to_string());
            InternalTopicSpec {
                name: topic.name.clone(),
                partitions: topic.partitions,
                replication_factor: topic.replication_factor.unwrap_or(0),
                configs,
            }
        })
        .collect()
}

/// Creates the topics in `specs` that the metadata of the controller does not
/// already hold.
///
/// This function mirrors `crate::txn::bootstrap::ensure_topic`. It assigns
/// replicas round-robin. It uses `spec.replication_factor` as the replication
/// factor if that value is `> 0`, and the configured default if not, with a
/// bound at the available brokers. It also writes a `V1TopicConfig` record when
/// the spec carries configs. The function tolerates `TopicExists`, which a
/// concurrent create causes.
///
/// The function re-reads the image and then returns the names of the topics
/// that are STILL absent after the attempt. The caller can then emit
/// `MISSING_INTERNAL_TOPICS` and keep the member `NotReady` until a later
/// heartbeat sees them.
/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
/// # Panics
/// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
pub async fn ensure_internal_topics(
    controller: &Arc<dyn MetadataSource>,
    specs: &[InternalTopicSpec],
    default_replication_factor: i16,
) -> Result<Vec<String>, BrokerError> {
    let image = controller.current_image();

    // Round-robin replica assignment needs the registered broker set.
    let mut brokers: Vec<NodeId> = image.brokers().map(|b| b.node_id).collect();
    brokers.sort_unstable();

    for spec in specs {
        if image.topic(&spec.name).is_some() {
            continue;
        }
        if spec.partitions <= 0 {
            continue;
        }
        // The client names these topics in its topology. Kafka's
        // `ConfiguredInternalTopic` refuses a name that `Topic.validate`
        // refuses, and the name becomes part of a partition directory path,
        // so such a topic is never created. It stays in the missing list.
        if let Err(invalid) = krabka_log::topic_name::validate_topic_name(&spec.name) {
            tracing::warn!(topic = %spec.name, error = %invalid, "refusing to create streams internal topic");
            continue;
        }
        if brokers.is_empty() {
            return Err(BrokerError::Txn(format!(
                "no brokers registered; cannot create internal topic '{}'",
                spec.name
            )));
        }

        let k = brokers.len();
        let rf_usize = streams_topic_replication_factor(
            spec.replication_factor,
            default_replication_factor,
            k,
        );
        let rf = i16::try_from(rf_usize).expect("rf <= brokers, fits i16");

        let mut records: Vec<MetadataRecord> = Vec::new();
        let topic_id = Uuid::new_v4();
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: spec.name.clone(),
            topic_id,
            partitions: spec.partitions,
            replication_factor: rf,
        }));

        for p in 0..spec.partitions {
            let mut replicas = Vec::with_capacity(rf_usize);
            let base = usize::try_from(p).expect("partition index fits in usize");
            for i in 0..rf_usize {
                replicas.push(brokers[(base + i) % k]);
            }
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: spec.name.clone(),
                partition: p,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        if !spec.configs.is_empty() {
            records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: spec.name.clone(),
                overrides: spec.configs.clone(),
            }));
        }

        match controller.submit_change(records).await {
            Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {}
            Err(e) => {
                return Err(BrokerError::Txn(format!(
                    "submit_change failed creating internal topic '{}': {e}",
                    spec.name
                )));
            }
        }
    }

    // Re-read the image; report whatever is still absent so the caller stays
    // NotReady until the create propagates.
    let after = controller.current_image();
    let still_missing = specs
        .iter()
        .filter(|s| after.topic(&s.name).is_none())
        .map(|s| s.name.clone())
        .collect();
    Ok(still_missing)
}

fn streams_topic_replication_factor(
    spec_replication_factor: i16,
    default_replication_factor: i16,
    broker_count: usize,
) -> usize {
    let desired = if spec_replication_factor > 0 {
        spec_replication_factor
    } else {
        default_replication_factor
    };
    crate::bootstrap::internal_topic_replication_factor(desired, broker_count)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::topology::configured::{
        ConfiguredInternalTopic, ConfiguredSubtopology,
    };

    #[test]
    fn internal_topic_specs_add_the_cleanup_policy_by_role() {
        let topic =
            |name: &str, replication_factor, configs: &[(&str, &str)]| ConfiguredInternalTopic {
                name: name.into(),
                partitions: 5,
                replication_factor,
                configs: configs
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            };
        let rp = topic("rp", Some(2), &[("segment.ms", "100")]);
        let cl = topic("cl", None, &[]);
        let configured = ConfiguredTopology {
            topology_epoch: 1,
            subtopologies: Some(BTreeMap::from([(
                "0".to_string(),
                ConfiguredSubtopology {
                    number_of_tasks: 5,
                    source_topics: BTreeSet::new(),
                    repartition_source_topics: BTreeMap::from([("rp".to_string(), rp.clone())]),
                    repartition_sink_topics: BTreeSet::new(),
                    state_changelog_topics: BTreeMap::from([("cl".to_string(), cl.clone())]),
                },
            )])),
            internal_topics_to_create: BTreeMap::from([
                ("cl".to_string(), cl),
                ("rp".to_string(), rp),
            ]),
            status: None,
        };

        assert!(
            internal_topic_specs(&configured)
                == vec![
                    InternalTopicSpec {
                        name: "cl".to_string(),
                        partitions: 5,
                        replication_factor: 0,
                        configs: maplit::btreemap! {
                            "cleanup.policy".to_string() => "compact".to_string()
                        },
                    },
                    InternalTopicSpec {
                        name: "rp".to_string(),
                        partitions: 5,
                        replication_factor: 2,
                        configs: maplit::btreemap! {
                            "cleanup.policy".to_string() => "delete".to_string(),
                            "segment.ms".to_string() => "100".to_string()
                        },
                    },
                ]
        );
    }

    #[test]
    fn configured_default_replication_factor_applies_when_spec_is_unspecified() {
        assert!(streams_topic_replication_factor(0, 2, 3) == 2);
    }
}
