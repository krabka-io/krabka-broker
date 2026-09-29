//! KIP-853 requests submitted to the single-owner Raft engine.

use krabka_metadata::Voter;

use crate::NodeId;

/// A request to add one voter. The candidate must already be a caught-up observer.
#[derive(Debug, Clone)]
pub struct AddVoter {
    pub voter: Voter,
    /// `AddRaftVoter` v1 may acknowledge after the local append so an
    /// auto-joining observer can keep fetching while the record commits.
    pub ack_when_committed: bool,
}

/// A request to remove one voter.
#[derive(Debug, Clone)]
pub struct RemoveVoter {
    pub id: NodeId,
    pub directory_id: uuid::Uuid,
}

/// A request to update one voter's endpoints / supported version range.
#[derive(Debug, Clone)]
pub struct UpdateVoter {
    pub voter: Voter,
}

/// A single KIP-853 control operation owned by the Raft engine.
#[derive(Debug, Clone)]
pub enum VoterChange {
    Add(AddVoter),
    /// Run only the checks `AddVoterHandler.handleAddVoterRequest` makes from
    /// the leader's own state, before it contacts the candidate: a pending
    /// change, an unset high watermark, `kraft.version`, an uncommitted voters
    /// record and a duplicate voter id. Nothing is appended, and an admitted
    /// request is answered [`ReconfigOutcome::Committed`]. The candidate's
    /// `ApiVersions` range and catch-up are the later, second half of the
    /// admission that [`VoterChange::Add`] still makes.
    CheckAdd(AddVoter),
    Remove(RemoveVoter),
    Update(UpdateVoter),
    FinalizeKraftVersion(u16),
}

/// Outcome shared by all three operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfigOutcome {
    Committed,
    NotLeader { leader: Option<NodeId> },
}
