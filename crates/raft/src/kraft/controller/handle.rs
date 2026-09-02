//! The cloneable [`KraftController`] handle: the accessors that read the
//! engine's published `watch` channels directly, and the command-sending
//! methods that round-trip a request through the engine task.

use std::sync::Arc;

use krabka_metadata::MetadataImage;
use krabka_units::prelude::ByteSize;
use tokio::sync::{oneshot, watch};

use super::KraftController;
use crate::{
    SubmitChangeResult,
    error::RaftError,
    kraft::{
        event::Event,
        transport::{Command, Inbound, MetadataFetchSlice, QuorumStateSnapshot},
        types::NodeId,
    },
};

impl KraftController {
    /// The node id this controller runs as.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.me
    }

    /// Probe a proposed voter through the controller's configured TLS/SASL
    /// dialer rather than opening an unauthenticated plaintext connection.
    pub(crate) async fn probe_kraft_version(
        &self,
        address: &str,
        finalized_version: u16,
    ) -> Result<bool, RaftError> {
        self.peers
            .probe_kraft_version(address, finalized_version)
            .await
    }

    /// A snapshot of the latest applied [`MetadataImage`].
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.image_rx.borrow().clone()
    }

    /// Watch the published [`MetadataImage`].
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_rx.clone()
    }

    /// Watch the current leader id.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_rx.clone()
    }

    /// A synchronous snapshot of consensus state (the handle's `quorum_state()`
    /// reads this without an mpsc round-trip — the engine republishes it on
    /// every event). Cheap `watch` borrow + clone.
    #[must_use]
    pub fn quorum_snapshot(&self) -> QuorumStateSnapshot {
        self.quorum_rx.borrow().clone()
    }

    /// Submit a metadata change. On the leader, appends the batch at the current
    /// leader epoch and returns once it is committed (HWM ≥ the appended end
    /// offset) AND applied, surfacing the first per-record rejection. On a
    /// follower, returns [`RaftError::NotLeader`] with the leader hint; the
    /// handle layer forwards via `forward_submit_to`.
    ///
    /// # Errors
    /// - [`RaftError::Metadata`] if a record fails `validate`.
    /// - [`RaftError::NotLeader`] if this node is not the leader.
    /// - [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn submit_change(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubmitChange { records, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// Submit generation-bound delegation-token mutations.
    ///
    /// # Errors
    ///
    /// Returns a leadership, shutdown, or mutation-rejection error.
    pub async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<crate::DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubmitDelegationTokenMutations { mutations, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// Submit one KIP-853 voter or kraft-version control operation.
    pub(crate) async fn reconfigure(
        &self,
        change: crate::reconfig::VoterChange,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Reconfigure { change, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// Atomically finalize `kraft.version` in the Raft control log.
    ///
    /// # Errors
    /// Returns an unsupported-version, leadership, timeout, or storage error.
    pub async fn finalize_kraft_version(
        &self,
        version: u16,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.reconfigure(crate::reconfig::VoterChange::FinalizeKraftVersion(version))
            .await
    }

    /// A structured snapshot of consensus state for the broker's
    /// `DescribeQuorum` admin view.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn quorum_state(&self) -> Result<QuorumStateSnapshot, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::QuorumStateSnapshot { reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    /// Read a committed `__cluster_metadata` slice for an observer's
    /// `API_KEY_METADATA_FETCH` (1004).
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn metadata_fetch(
        &self,
        fetch_offset: i64,
        max_size: ByteSize,
    ) -> Result<MetadataFetchSlice, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MetadataFetch {
                fetch_offset,
                max_size,
                reply,
            })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    /// Serialize the current image to a KIP-630 checkpoint under the data dir.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::TriggerSnapshot { reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// Inject a raw core [`Event`] into the loop (test/driver entrypoint and the
    /// internal feedback path for peer-RPC responses).
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn inject_event(&self, event: Event) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Event(event))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Deliver an inbound peer RPC to the engine.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn deliver(&self, inbound: Inbound) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Inbound(inbound))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Stop the engine task.
    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(Command::Shutdown).await;
    }

    /// Test-only: append `records` as a committed batch and apply them through
    /// the real pipeline; returns the appended base offset.
    #[cfg(test)]
    pub(super) async fn test_append_and_commit(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<i64, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::TestAppendAndCommit { records, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}
