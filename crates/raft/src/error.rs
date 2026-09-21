use thiserror::Error;

use crate::types::NodeId;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RaftError {
    #[error("storage: {0}")]
    Storage(#[from] krabka_log::LogError),

    #[error("network: {0}")]
    Network(#[from] krabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] krabka_protocol::ProtocolError),

    #[error("controller admin dispatch: {0}")]
    ControllerAdmin(String),

    #[error("records: {0}")]
    Records(#[from] krabka_protocol::records::RecordsError),

    #[error("metadata: {0}")]
    Metadata(#[from] krabka_metadata::MetadataError),

    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    #[error("not leader; current leader: {current_leader:?}")]
    NotLeader { current_leader: Option<NodeId> },

    #[error("leader unknown (election in progress)")]
    LeaderUnknown,

    #[error("change rejected: {0}")]
    ChangeRejected(String),

    /// The leader refused a compare-and-set because its log holds records
    /// that are not committed yet.
    ///
    /// A break-glass consume and a topic-freeze replacement check the
    /// committed image, so the leader decides them only when its whole log is
    /// committed. A newly elected leader is in this state until it commits a
    /// record from its own epoch. Kafka's controller is not active in the same
    /// window, and it answers `NOT_CONTROLLER`. The refusal clears when the
    /// tail commits, so a caller can retry.
    #[error("the controller leader has uncommitted metadata records")]
    UncommittedTail,

    /// The controller refused a request because the principal of the
    /// connection lacks the cluster operation that the request needs.
    #[error("the controller denied the cluster operation to this node's principal")]
    ClusterAuthorizationFailed,

    #[error("reconfiguration rejected: {0}")]
    ReconfigRejected(String),

    #[error("a reconfiguration is already in progress")]
    ReconfigInProgress,

    #[error("voter {0} already exists")]
    DuplicateVoter(NodeId),

    #[error("voter {0} was not found")]
    VoterNotFound(NodeId),

    #[error("invalid voter update: {0}")]
    InvalidVoterUpdate(String),

    #[error("kraft.version {0} does not permit dynamic voter changes")]
    UnsupportedKraftVersion(u16),

    #[error("voter {id} is not a caught-up observer (lag {lag})")]
    VoterNotCaughtUp { id: NodeId, lag: u64 },

    #[error("serialization: {0}")]
    SerdeFailed(#[from] wincode::error::WriteError),

    #[error("deserialization: {0}")]
    SerdeFailedDecode(#[from] wincode::error::ReadError),

    #[error("startup misconfiguration: {0}")]
    Startup(String),

    #[error("controller shut down")]
    Shutdown,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_not_leader_with_id() {
        let e = RaftError::NotLeader {
            current_leader: Some(NodeId(7)),
        };
        assert2::assert!(e.to_string().contains("Some(NodeId(7))"));
    }

    #[test]
    fn display_not_leader_without_id() {
        let e = RaftError::NotLeader {
            current_leader: None,
        };
        assert2::assert!(e.to_string().contains("None"));
    }
}
