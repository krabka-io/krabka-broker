//! Translation of the KIP-853 voter set between its `krabka-metadata` form and
//! the `VotersRecord` control record a snapshot carries on disk.
//!
//! The writer and the reader both need this translation and neither owns it, so
//! it lives in its own module. Every range check that rejects a malformed voter
//! id or `kraft.version` bound is here.

use krabka_metadata::{NodeId, Voter, VoterEndpoint, VoterSet, voters::KRaftVersionRange};
use krabka_protocol::owned::voters_record::{
    Endpoint as WireVoterEndpoint, KRaftVersionFeature as WireKRaftVersionFeature,
    Voter as WireVoter, VotersRecord as WireVotersRecord,
};
use uuid::Uuid;

use crate::error::RaftError;

pub(super) fn voter_set_to_wire(voters: &VoterSet) -> Result<WireVotersRecord, RaftError> {
    let voters = voters
        .iter()
        .map(|voter| {
            Ok(WireVoter {
                voter_id: i32::try_from(voter.id.0).map_err(|_| {
                    RaftError::ChangeRejected("snapshot voter id exceeds int32".into())
                })?,
                voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                    *voter.directory_id.as_bytes(),
                ),
                endpoints: voter
                    .endpoints
                    .iter()
                    .map(|endpoint| WireVoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                        ..Default::default()
                    })
                    .collect(),
                k_raft_version_feature: WireKRaftVersionFeature {
                    min_supported_version: i16::try_from(voter.kraft_version.min).map_err(
                        |_| {
                            RaftError::ChangeRejected(
                                "snapshot minimum kraft.version exceeds int16".into(),
                            )
                        },
                    )?,
                    max_supported_version: i16::try_from(voter.kraft_version.max).map_err(
                        |_| {
                            RaftError::ChangeRejected(
                                "snapshot maximum kraft.version exceeds int16".into(),
                            )
                        },
                    )?,
                    ..Default::default()
                },
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    Ok(WireVotersRecord {
        version: 0,
        voters,
        ..Default::default()
    })
}

pub(super) fn voter_set_from_wire(record: &WireVotersRecord) -> Result<VoterSet, RaftError> {
    let voters = record
        .voters
        .iter()
        .map(|voter| {
            let id = u64::try_from(voter.voter_id)
                .map_err(|_| RaftError::ChangeRejected("negative voter id in snapshot".into()))?;
            let min = u16::try_from(voter.k_raft_version_feature.min_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative minimum kraft.version in snapshot".into()),
            )?;
            let max = u16::try_from(voter.k_raft_version_feature.max_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative maximum kraft.version in snapshot".into()),
            )?;
            if min > max {
                return Err(RaftError::ChangeRejected(
                    "inverted kraft.version range in snapshot".into(),
                ));
            }
            Ok(Voter {
                id: NodeId(id),
                directory_id: Uuid::from_bytes(voter.voter_directory_id.0),
                endpoints: voter
                    .endpoints
                    .iter()
                    .map(|endpoint| VoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                    })
                    .collect(),
                kraft_version: KRaftVersionRange { min, max },
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    let voter_count = voters.len();
    let voters = VoterSet::from_voters(voters);
    if voters.len() != voter_count {
        return Err(RaftError::ChangeRejected(
            "duplicate voter id in snapshot".into(),
        ));
    }
    Ok(voters)
}
