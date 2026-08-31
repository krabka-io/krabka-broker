//! `MetadataSource`: the metadata authority a broker reads from and
//! writes through.
//!
//! Combined and controller nodes back it with a live `ControllerHandle`,
//! which is an openraft voter. Broker-only nodes back it with a
//! `MetadataObserver`, a true `KRaft` observer, plus a write-forwarding
//! path to the controller quorum. Handlers depend only on this trait.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;

mod controller_handle;
mod image_watch;
mod observer_source;
mod quorum_forwarder;
#[cfg(test)]
mod test_support;

pub(crate) use self::image_watch::watch_image_loop;
pub use self::{observer_source::ObserverSource, quorum_forwarder::QuorumForwarder};

#[async_trait::async_trait]
pub trait MetadataSource: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>>;
    fn quorum_state(&self) -> QuorumState;
    /// Highest metadata-log offset applied to the current image, or `-1`
    /// before the first record.
    fn current_metadata_offset(&self) -> i64 {
        i64::try_from(self.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1)
    }
    /// Highest `__cluster_metadata` offset the metadata quorum has committed,
    /// as this node last heard it, or `-1` before it has heard anything.
    ///
    /// [`Self::current_metadata_offset`] says how far this node has got;
    /// this says how far the quorum has got. The gap between the two is the
    /// metadata lag the readiness probe bounds. The default treats the node's
    /// own applied offset as the quorum's, which is what a source with no
    /// separate view of the quorum can honestly report.
    fn quorum_committed_offset(&self) -> i64 {
        self.current_metadata_offset()
    }
    /// Directory identity voted for in the current controller epoch.
    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        None
    }
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError>;
    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError>;
    /// The controller listener's bound address. It is meaningful only on
    /// controller and combined nodes. Broker-only observers have no
    /// controller listener and report an unspecified address.
    fn controller_bound_addr(&self) -> SocketAddr;
    /// Read a byte window of the latest metadata snapshot to serve
    /// `FetchSnapshot`. Controller and combined nodes back this with their
    /// on-disk checkpoint. Broker-only observers have none to serve.
    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange;
    /// Schedule a metadata snapshot. It is meaningful only on controller and
    /// combined nodes. Broker-only observers have no log of their own to
    /// snapshot.
    async fn trigger_snapshot(&self) -> Result<(), RaftError>;
    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn finalize_kraft_version(&self, _version: u16) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn cancel(&self);
}

/// Write side for broker-only nodes: forward a batch to the controller
/// quorum leader.
#[async_trait::async_trait]
pub trait MetadataWriter: Send + Sync {
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
}
