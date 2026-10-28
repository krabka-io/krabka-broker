//! Kafka's `StreamsGroupTopologyDescription`, and its
//! `StreamsGroupTopologyDescriptionConverter` to and from the wire.
//!
//! A push carries every field on every node. The broker keeps only the fields
//! of the node's type, as Kafka's converter does: a source keeps its topics, a
//! processor its stores, a sink its topic. Each list keeps the first of
//! repeated names in wire order, as Kafka's `LinkedHashSet` keeps them, so a
//! describe answers the description Kafka would store, not the bytes the
//! client sent.

use krabka_protocol::owned::common::{
    streams_group_describe_response as describe,
    streams_group_topology_description_update_request as push,
};

/// Kafka's `NODE_TYPE_SOURCE`.
const NODE_TYPE_SOURCE: i8 = 1;
/// Kafka's `NODE_TYPE_PROCESSOR`.
const NODE_TYPE_PROCESSOR: i8 = 2;
/// Kafka's `NODE_TYPE_SINK`.
const NODE_TYPE_SINK: i8 = 3;

/// A Streams topology as the plugin stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyDescription {
    pub subtopologies: Vec<Subtopology>,
    pub global_stores: Vec<GlobalStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subtopology {
    pub id: String,
    pub nodes: Vec<Node>,
}

/// One processing node. The wire carries only the successors; a reader
/// rebuilds the predecessors from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    pub kind: NodeKind,
    pub successors: Vec<String>,
}

/// What a [`Node`] keeps of its type: Kafka's `Source`, `Processor` and
/// `Sink` records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    Source { topics: Vec<String> },
    Processor { stores: Vec<String> },
    Sink { topic: Option<String> },
}

/// A global store: a source node that feeds a processor node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalStore {
    pub source: Node,
    pub processor: Node,
}

impl TopologyDescription {
    /// Kafka's `StreamsGroupTopologyDescriptionConverter.fromRequest`.
    ///
    /// # Errors
    ///
    /// Returns the message of Kafka's `InvalidRequestException` for a node of
    /// an unknown type, or for a global store that is not a source and a
    /// processor. The subtopologies are read before the global stores, so the
    /// first such node in that order names the error.
    pub fn from_push(
        wire: &push::topology_description::TopologyDescription,
    ) -> Result<Self, String> {
        let subtopologies = wire
            .subtopologies
            .iter()
            .map(|subtopology| {
                Ok(Subtopology {
                    id: subtopology.subtopology_id.clone(),
                    nodes: subtopology
                        .nodes
                        .iter()
                        .map(Node::from_push)
                        .collect::<Result<_, String>>()?,
                })
            })
            .collect::<Result<_, String>>()?;
        let global_stores = wire
            .global_stores
            .iter()
            .map(|store| {
                let source = Node::from_push(&store.source)?;
                let processor = Node::from_push(&store.processor)?;
                match (&source.kind, &processor.kind) {
                    (NodeKind::Source { .. }, NodeKind::Processor { .. }) => {
                        Ok(GlobalStore { source, processor })
                    }
                    _ => Err(
                        "Global store must be composed of a source and a processor node."
                            .to_owned(),
                    ),
                }
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            subtopologies,
            global_stores,
        })
    }

    /// Kafka's `StreamsGroupTopologyDescriptionConverter.toDescribeResponse`.
    #[must_use]
    pub fn to_describe(&self) -> describe::topology_description::TopologyDescription {
        describe::topology_description::TopologyDescription {
            subtopologies: self
                .subtopologies
                .iter()
                .map(|subtopology| {
                    describe::topology_description_subtopology::TopologyDescriptionSubtopology {
                        subtopology_id: subtopology.id.clone(),
                        nodes: subtopology.nodes.iter().map(Node::to_describe).collect(),
                        ..Default::default()
                    }
                })
                .collect(),
            global_stores: self
                .global_stores
                .iter()
                .map(|store| {
                    describe::topology_description_global_store::TopologyDescriptionGlobalStore {
                        source: store.source.to_describe(),
                        processor: store.processor.to_describe(),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        }
    }
}

impl Node {
    fn from_push(
        wire: &push::topology_description_node::TopologyDescriptionNode,
    ) -> Result<Self, String> {
        let kind = match wire.node_type {
            NODE_TYPE_SOURCE => NodeKind::Source {
                topics: first_of_each(&wire.source_topics),
            },
            NODE_TYPE_PROCESSOR => NodeKind::Processor {
                stores: first_of_each(&wire.stores),
            },
            NODE_TYPE_SINK => NodeKind::Sink {
                topic: wire.sink_topic.clone(),
            },
            other => return Err(format!("Unknown topology node type: {other}")),
        };
        Ok(Self {
            name: wire.name.clone(),
            kind,
            successors: first_of_each(&wire.successors),
        })
    }

    fn to_describe(&self) -> describe::topology_description_node::TopologyDescriptionNode {
        let mut node = describe::topology_description_node::TopologyDescriptionNode {
            name: self.name.clone(),
            successors: self.successors.clone(),
            ..Default::default()
        };
        match &self.kind {
            NodeKind::Source { topics } => {
                node.node_type = NODE_TYPE_SOURCE;
                node.source_topics.clone_from(topics);
            }
            NodeKind::Processor { stores } => {
                node.node_type = NODE_TYPE_PROCESSOR;
                node.stores.clone_from(stores);
            }
            NodeKind::Sink { topic } => {
                node.node_type = NODE_TYPE_SINK;
                node.sink_topic.clone_from(topic);
            }
        }
        node
    }
}

/// `names` without repeats, each at its first position: the iteration order of
/// Kafka's `new LinkedHashSet<>(names)`.
fn first_of_each(names: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = Vec::with_capacity(names.len());
    for name in names {
        if !kept.contains(name) {
            kept.push(name.clone());
        }
    }
    kept
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn push_node(
        name: &str,
        node_type: i8,
    ) -> push::topology_description_node::TopologyDescriptionNode {
        push::topology_description_node::TopologyDescriptionNode {
            name: name.into(),
            node_type,
            // Every field set, so the conversion shows what each type keeps.
            source_topics: vec!["in".into(), "in-b".into(), "in".into()],
            sink_topic: Some("out".into()),
            stores: vec!["store".into(), "store".into()],
            successors: vec!["next".into(), "next".into()],
            ..Default::default()
        }
    }

    #[derive(Clone, Copy, Default)]
    enum DescribedNodeKind {
        #[default]
        Source,
        Processor,
        Sink,
    }

    impl DescribedNodeKind {
        fn wire_code(self) -> i8 {
            match self {
                Self::Source => NODE_TYPE_SOURCE,
                Self::Processor => NODE_TYPE_PROCESSOR,
                Self::Sink => NODE_TYPE_SINK,
            }
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct DescribedNodeSetup<'a> {
        #[default("node")]
        name: &'a str,
        kind: DescribedNodeKind,
        source_topics: &'a [&'a str],
        sink_topic: Option<&'a str>,
        stores: &'a [&'a str],
    }

    #[bon::builder]
    fn describe_node(
        #[builder(default = "node")] name: &'_ str,
        #[builder(default)] kind: DescribedNodeKind,
        #[builder(default)] source_topics: &'_ [&'_ str],
        sink_topic: Option<&'_ str>,
        #[builder(default)] stores: &'_ [&'_ str],
    ) -> describe::topology_description_node::TopologyDescriptionNode {
        let setup = DescribedNodeSetup {
            name,
            kind,
            source_topics,
            sink_topic,
            stores,
        };
        let DescribedNodeSetup {
            name,
            kind,
            source_topics,
            sink_topic,
            stores,
        } = setup;
        describe::topology_description_node::TopologyDescriptionNode {
            name: name.into(),
            node_type: kind.wire_code(),
            source_topics: source_topics.iter().map(|&t| t.to_owned()).collect(),
            sink_topic: sink_topic.map(str::to_owned),
            stores: stores.iter().map(|&s| s.to_owned()).collect(),
            successors: vec!["next".into()],
            ..Default::default()
        }
    }

    /// A push round-trips to the describe response Kafka builds from the
    /// plugin's copy: each node keeps the fields of its type, and every list
    /// keeps its first occurrences in wire order.
    /// The pushes Kafka's converter refuses with `InvalidRequestException`,
    /// and its messages.
    use {
        describe::topology_description::TopologyDescription as DescribedTopology,
        push::topology_description::TopologyDescription as PushTopology,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    #[derive(Default)]
    struct TopologyPush {
        wire: Option<PushTopology>,
        description: Option<Result<DescribedTopology, String>>,
    }
    #[rstest::fixture]
    fn topology_push() -> TopologyPush {
        // Start this scenario with fresh inputs and observations.
        TopologyPush::default()
    }
    #[given(
        "a topology push has source processor and sink nodes plus a source-processor global store"
    )]
    fn valid_topology_push(topology_push: &mut TopologyPush) {
        let wire = push::topology_description::TopologyDescription {
            subtopologies: vec![
                push::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![
                        push_node("KSTREAM-SOURCE-0000000000", NODE_TYPE_SOURCE),
                        push_node("KSTREAM-MAPVALUES-0000000001", NODE_TYPE_PROCESSOR),
                        push_node("KSTREAM-SINK-0000000002", NODE_TYPE_SINK),
                    ],
                    ..Default::default()
                },
            ],
            global_stores: vec![
                push::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: push_node("KSTREAM-SOURCE-0000000003", NODE_TYPE_SOURCE),
                    processor: push_node("KTABLE-SOURCE-0000000004", NODE_TYPE_PROCESSOR),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        topology_push.wire = Some(wire);
    }
    #[given(
        "a topology push has subtopology node type {node} global source type {source} and global processor type {processor}"
    )]
    fn invalid_topology_push(
        topology_push: &mut TopologyPush,
        node: String,
        source: String,
        processor: String,
    ) {
        let mut wire = PushTopology::default();
        if node != "absent" {
            wire.subtopologies = vec![
                push::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![push_node("n", node.parse().expect("node type"))],
                    ..Default::default()
                },
            ];
        }
        if source != "absent" {
            wire.global_stores = vec![
                push::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: push_node("s", source.parse().expect("source type")),
                    processor: push_node("p", processor.parse().expect("processor type")),
                    ..Default::default()
                },
            ];
        }
        topology_push.wire = Some(wire);
    }
    #[when("the coordinator validates the pushed topology and renders its describe response")]
    fn describe_pushed_topology(topology_push: &mut TopologyPush) {
        topology_push.description = Some(
            TopologyDescription::from_push(topology_push.wire.as_ref().expect("push prepared"))
                .map(|description| description.to_describe()),
        );
    }
    #[then(
        "the describe response preserves every node field and deduplicates each list in original order"
    )]
    fn described_topology(topology_push: &TopologyPush) {
        let expected = describe::topology_description::TopologyDescription {
            subtopologies: vec![
                describe::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![
                        describe_node()
                            .name("KSTREAM-SOURCE-0000000000")
                            .source_topics(&["in", "in-b"])
                            .call(),
                        describe_node()
                            .name("KSTREAM-MAPVALUES-0000000001")
                            .kind(DescribedNodeKind::Processor)
                            .stores(&["store"])
                            .call(),
                        describe_node()
                            .name("KSTREAM-SINK-0000000002")
                            .kind(DescribedNodeKind::Sink)
                            .maybe_sink_topic(Some("out"))
                            .call(),
                    ],
                    ..Default::default()
                },
            ],
            global_stores: vec![
                describe::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: describe_node()
                        .name("KSTREAM-SOURCE-0000000003")
                        .source_topics(&["in", "in-b"])
                        .call(),
                    processor: describe_node()
                        .name("KTABLE-SOURCE-0000000004")
                        .kind(DescribedNodeKind::Processor)
                        .stores(&["store"])
                        .call(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(
            topology_push
                .description
                .as_ref()
                .expect("push handled")
                .as_ref()
                .expect("valid pushed topology")
                == &expected
        );
    }
    #[then("the topology push is rejected with Kafka message {message}")]
    fn refused_topology_push(topology_push: &TopologyPush, message: String) {
        assert!(
            topology_push
                .description
                .as_ref()
                .expect("push handled")
                .as_ref()
                .err()
                == Some(&message)
        );
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/streams/description/model.feature",fixtures=[topology_push:TopologyPush]);
}
