//! Internal-topic specs and their materialization.
//!
//! A topology needs a repartition topic for every `repartition_source_topics`
//! entry and a changelog topic for every `state_changelog_topics` entry. This
//! module computes those specs from the derived task counts, and it is the one
//! place in topology handling that writes metadata records through the
//! controller.

use std::{collections::BTreeMap, sync::Arc};

use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord};
use krabka_raft::RaftError;
use uuid::Uuid;

use crate::{
    coordinator::unified::streams::persistence::{StoredTopicInfo, StreamsGroupTopologyValue},
    error::BrokerError,
    metadata_source::MetadataSource,
};

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

/// Computes the internal repartition and changelog topics that the topology
/// needs.
///
/// The derived task count of the owning subtopology sizes each topic. A
/// changelog topic gets `cleanup.policy=compact`. A repartition topic gets
/// `cleanup.policy=delete`. Each policy layers on top of the configs that the
/// client supplied. This function de-duplicates by name, and the first
/// occurrence wins. A subtopology with an unresolved task count contributes no
/// spec, because this function cannot size it yet.
#[must_use]
pub fn required_internal_topics(
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) -> Vec<InternalTopicSpec> {
    let mut by_name: BTreeMap<String, InternalTopicSpec> = BTreeMap::new();

    for sub in &topology.subtopologies {
        let Some(&partitions) = num_tasks.get(&sub.subtopology_id) else {
            continue;
        };
        if partitions <= 0 {
            continue;
        }

        for info in &sub.repartition_source_topics {
            add_spec(&mut by_name, info, partitions, "delete");
        }
        for info in &sub.state_changelog_topics {
            add_spec(&mut by_name, info, partitions, "compact");
        }
    }

    by_name.into_values().collect()
}

fn add_spec(
    by_name: &mut BTreeMap<String, InternalTopicSpec>,
    info: &StoredTopicInfo,
    partitions: i32,
    cleanup_policy: &str,
) {
    if by_name.contains_key(&info.name) {
        return;
    }
    let mut configs: BTreeMap<String, String> = info.topic_configs.iter().cloned().collect();
    configs
        .entry("cleanup.policy".to_string())
        .or_insert_with(|| cleanup_policy.to_string());
    by_name.insert(
        info.name.clone(),
        InternalTopicSpec {
            name: info.name.clone(),
            partitions,
            replication_factor: info.replication_factor,
            configs,
        },
    );
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
    use crate::coordinator::unified::streams::topology::test_support::sub;

    #[test]
    fn required_internal_topics_sizes_and_configs() {
        let mut s0 = sub("0");
        s0.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0,
            replication_factor: 2,
            topic_configs: vec![("segment.ms".into(), "100".into())],
        }];
        s0.state_changelog_topics = vec![StoredTopicInfo {
            name: "cl".into(),
            partitions: 0,
            replication_factor: 3,
            topic_configs: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let mut num_tasks = BTreeMap::new();
        num_tasks.insert("0".to_string(), 5);

        let specs = required_internal_topics(&topology, &num_tasks);
        assert!(specs.len() == 2);

        let rp = specs.iter().find(|s| s.name == "rp").unwrap();
        assert!(
            *rp == InternalTopicSpec {
                name: "rp".to_string(),
                partitions: 5,
                replication_factor: 2,
                configs: maplit::btreemap! {
                "cleanup.policy".to_string() => "delete".to_string(),
                "segment.ms".to_string() => "100".to_string()},
            }
        );

        let cl = specs.iter().find(|s| s.name == "cl").unwrap();
        assert!(
            *cl == InternalTopicSpec {
                name: "cl".to_string(),
                partitions: 5,
                replication_factor: 3,
                configs: maplit::btreemap! {"cleanup.policy".to_string() => "compact".to_string()},
            }
        );
    }

    #[test]
    fn required_internal_topics_skips_unresolved_subtopology() {
        let mut s0 = sub("0");
        s0.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        // No entry for subtopology "0" -> unresolved -> no specs.
        let specs = required_internal_topics(&topology, &BTreeMap::new());
        assert!(specs.is_empty());
    }

    #[test]
    fn configured_default_replication_factor_applies_when_spec_is_unspecified() {
        assert!(streams_topic_replication_factor(0, 2, 3) == 2);
    }
}
