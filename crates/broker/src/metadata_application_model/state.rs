//! The search space: the committed metadata log, and for each node its metadata
//! image, its checkpoint, the image its replicator supervisor last reconciled,
//! the partition's installed replication target, and the replica fetcher's
//! target.
//!
//! Every value is a small integer or a bitset, so a state hashes cheaply. The
//! one exception is the log, which holds the record bytes the controller
//! encoded: the bytes are what a replay decodes, and the model needs them
//! because the decode, not the record the controller meant, is what diverges.

use bytes::Bytes;

/// A node id. Node ids are 1-based, as the brokers' are.
pub(super) type Node = u8;

/// The three nodes, each a combined broker and controller.
pub(super) const NODES: [Node; 3] = [1, 2, 3];

/// The nodes that host a replica of the one partition, in assignment order.
pub(super) const REPLICAS: [Node; 2] = [1, 2];

/// The active controller, which hosts no replica of the partition. The model
/// never crashes it: its image is the committed one, the image every node
/// would hold if it replayed the log once.
pub(super) const CONTROLLER: Node = 3;

/// A set of nodes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct Members(u8);

impl Members {
    pub(super) fn of(nodes: impl IntoIterator<Item = Node>) -> Self {
        Self(nodes.into_iter().fold(0, |bits, node| bits | (1 << node)))
    }

    pub(super) const fn of_const(bits: u8) -> Self { Self(bits) }

    pub(super) const fn contains(self, node: Node) -> bool { self.0 & (1 << node) != 0 }

    pub(super) fn iter(self) -> impl Iterator<Item = Node> {
        NODES.into_iter().filter(move |node| self.contains(*node))
    }
}

/// What a metadata image holds for the partition: the projection of
/// `PartitionRecord` and the leader recovery state that the model tracks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct View {
    pub(super) leader: Node,
    pub(super) leader_epoch: u8,
    pub(super) isr: Members,
    pub(super) partition_epoch: u8,
    pub(super) recovering: bool,
}

impl View {
    /// The replication target this image names.
    pub(super) const fn target(self) -> Target {
        Target {
            leader: self.leader,
            leader_epoch: self.leader_epoch,
        }
    }
}

/// A leader and leader epoch: the partition's installed `ReplicationTarget`,
/// or the target a replica fetcher follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct Target {
    pub(super) leader: Node,
    pub(super) leader_epoch: u8,
}

/// The metadata checkpoint a node restarts from, and the number of committed
/// batches below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct Checkpoint {
    pub(super) view: View,
    pub(super) offset: u8,
}

/// One node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct NodeState {
    /// The node's metadata image, as its `KRaft` replica applied it.
    pub(super) image: View,
    /// The committed batches the image has applied.
    pub(super) applied: u8,
    /// The newest checkpoint on disk.
    pub(super) checkpoint: Checkpoint,
    /// The image the supervisor last took from the watch channel, `None` after
    /// a restart until the first pass takes one.
    pub(super) seen: Option<View>,
    /// The partition's installed `ReplicationTarget`, `None` after a restart
    /// until the first pass installs one.
    pub(super) installed: Option<Target>,
    /// The target this node's replica fetcher follows, `None` while it leads.
    pub(super) fetcher: Option<Target>,
    /// Whether this node's last fetch from `fetcher` appended: the leader's
    /// record that the follower has caught up at its epoch.
    pub(super) caught_up: bool,
}

impl NodeState {
    pub(super) const fn new(view: View) -> Self {
        Self {
            image: view,
            applied: 0,
            checkpoint: Checkpoint { view, offset: 0 },
            seen: Some(view),
            installed: Some(view.target()),
            fetcher: None,
            caught_up: false,
        }
    }
}

/// One committed metadata batch: the value bytes of its records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Batch(pub(super) Vec<Bytes>);

/// Events the `sometimes` properties ask for, kept as bits so a witness is a
/// property of a state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct Witnessed(u8);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Event {
    /// A restart plan committed an unclean election.
    UncleanElection = 1,
    /// A node restarted from a checkpoint with committed batches below it.
    ReplayAboveCheckpoint = 2,
}

impl Witnessed {
    pub(super) const fn with(self, event: Event) -> Self { Self(self.0 | event as u8) }

    pub(super) const fn has(self, event: Event) -> bool { self.0 & event as u8 != 0 }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct State {
    pub(super) log: Vec<Batch>,
    pub(super) nodes: [NodeState; 3],
    /// Crash-restarts injected so far.
    pub(super) restarts: u8,
    pub(super) witnessed: Witnessed,
}

impl State {
    pub(super) fn node(&self, node: Node) -> &NodeState { &self.nodes[index(node)] }

    pub(super) fn node_mut(&mut self, node: Node) -> &mut NodeState { &mut self.nodes[index(node)] }

    /// The committed image: the controller's.
    pub(super) fn committed(&self) -> View { self.node(CONTROLLER).image }
}

fn index(node: Node) -> usize { usize::from(node - 1) }
