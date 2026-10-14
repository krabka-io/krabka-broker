use {
    krabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint},
    krabka_raft::NodeId,
    uuid::Uuid,
};

/// Build the controller voter shared by the quorum example and reconfig suite.
pub fn voter(id: u64, directory_id: Uuid) -> Voter {
    Voter {
        id: NodeId(id),
        directory_id,
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
        }],
        kraft_version: KRaftVersionRange { min: 0, max: 1 },
    }
}
