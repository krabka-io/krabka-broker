//! The voter-administration surface of [`ControllerHandle`]: the KIP-853 add,
//! remove and update operations, the `kraft.version` finalization, the staging
//! area for an observer awaiting promotion, and the whole-set membership delta
//! that is reconciled into a single one of those engine commands.

use super::ControllerHandle;
use crate::{
    error::RaftError,
    types::{Node, NodeId},
};

impl ControllerHandle {
    /// Reconcile a single-node voter-set delta through the KIP-853 engine.
    ///
    /// # Errors
    /// Rejects multi-node batch changes; KIP-642 is a separate operation.
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        let current = self.engine.quorum_snapshot().voters;
        let current_ids: std::collections::BTreeSet<NodeId> = current.ids().into_iter().collect();
        let added: Vec<NodeId> = new_voters.difference(&current_ids).copied().collect();
        let removed: Vec<NodeId> = current_ids.difference(&new_voters).copied().collect();
        if added.len() + removed.len() > 1 {
            return Err(RaftError::ReconfigRejected(
                "only one voter change may be submitted at a time".into(),
            ));
        }
        let outcome = if let Some(id) = removed.first() {
            let voter = current.get(*id).ok_or_else(|| {
                RaftError::ReconfigRejected(format!(
                    "voter {id} disappeared while preparing removal"
                ))
            })?;
            self.remove_voter(crate::reconfig::RemoveVoter {
                id: *id,
                directory_id: voter.directory_id,
            })
            .await?
        } else if let Some(id) = added.first() {
            let node = self
                .staged_learners
                .lock()
                .map_err(|_| RaftError::ReconfigRejected("staged learner lock poisoned".into()))?
                .get(id)
                .cloned()
                .ok_or_else(|| {
                    RaftError::ReconfigRejected(format!(
                        "voter {id} must be staged with add_learner first"
                    ))
                })?;
            self.add_voter(crate::reconfig::AddVoter {
                voter: krabka_metadata::Voter {
                    id: *id,
                    directory_id: node.directory_id,
                    endpoints: node.endpoints,
                    kraft_version: node.kraft_version,
                },
                ack_when_committed: true,
            })
            .await?
        } else {
            return Ok(());
        };
        match outcome {
            crate::reconfig::ReconfigOutcome::Committed => Ok(()),
            crate::reconfig::ReconfigOutcome::NotLeader { leader } => Err(RaftError::NotLeader {
                current_leader: leader,
            }),
        }
    }

    /// Stage a caught-up observer identity for later voter promotion.
    ///
    /// # Errors
    /// The observer catches up by fetching from the leader; this call does not
    /// alter quorum membership.
    pub fn add_learner(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> std::future::Ready<Result<(), RaftError>> {
        let result = self
            .staged_learners
            .lock()
            .map_err(|_| RaftError::ReconfigRejected("staged learner lock poisoned".into()))
            .map(|mut learners| {
                learners.insert(node_id, node);
            });
        std::future::ready(result)
    }

    /// Add a caught-up controller voter through the Raft control log.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn add_voter(
        &self,
        req: crate::reconfig::AddVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Add(req))
            .await
    }

    /// Remove the exact node/directory pair through the Raft control log.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn remove_voter(
        &self,
        req: crate::reconfig::RemoveVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Remove(req))
            .await
    }

    /// Update the exact voter's endpoint and supported feature range.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn update_voter(
        &self,
        req: crate::reconfig::UpdateVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Update(req))
            .await
    }

    /// Atomically append `KRaftVersionRecord` and the initial `VotersRecord`.
    ///
    /// # Errors
    /// Returns an unsupported-version, leadership, timeout, or storage error.
    pub async fn finalize_kraft_version(
        &self,
        version: u16,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine.finalize_kraft_version(version).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::TempDir;

    use super::*;
    use crate::{config::ControllerConfig, controller::Controller};

    #[tokio::test]
    async fn change_membership_validates_delta_count() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        let ctrl = Controller::start(cfg).await.expect("bootstrap");

        // 0 changes: current is {1}, target is {1} -> Ok(())
        let res_zero = ctrl.change_membership(BTreeSet::from([NodeId(1)])).await;
        assert2::assert!(res_zero.is_ok());

        // 1 added + 1 removed = 2 changes: {1} -> {2} -> ReconfigRejected
        let res_swap = ctrl.change_membership(BTreeSet::from([NodeId(2)])).await;
        assert2::assert!(matches!(
            res_swap,
            Err(RaftError::ReconfigRejected(ref msg)) if msg.contains("only one voter change")
        ));

        // 2 added = 2 changes: {1} -> {1, 2, 3} -> ReconfigRejected
        let res_multi_add = ctrl
            .change_membership(BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]))
            .await;
        assert2::assert!(matches!(
            res_multi_add,
            Err(RaftError::ReconfigRejected(ref msg)) if msg.contains("only one voter change")
        ));

        // 1 change: {1} -> {1, 2} -> fails because NodeId(2) is not staged as learner
        let res_single_add = ctrl
            .change_membership(BTreeSet::from([NodeId(1), NodeId(2)]))
            .await;
        assert2::assert!(matches!(
            res_single_add,
            Err(RaftError::ReconfigRejected(ref msg)) if msg.contains("must be staged with add_learner first")
        ));

        ctrl.shutdown().await;
    }
}
