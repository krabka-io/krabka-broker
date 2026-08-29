//! The KIP-853 control state: the uncommitted-through-committed history of
//! voter sets and `kraft.version` levels, together with the conversions between
//! that history and the `VotersRecord` wire shape it is written to the log in.

use std::collections::BTreeMap;

use krabka_ids::Offset;
use krabka_metadata::VoterSet;
use krabka_protocol::{
    owned::voters_record::{
        Endpoint as WireVoterEndpoint, KRaftVersionFeature as WireKRaftVersionFeature,
        Voter as WireVoter, VotersRecord as WireVotersRecord,
    },
    records::metadata::control::ControlRecord,
};
use uuid::Uuid;

use super::KraftControlState;
use crate::{error::RaftError, kraft::types::NodeId};

impl KraftControlState {
    pub fn new(voters: VoterSet, version: u16) -> Self {
        Self {
            voter_history: BTreeMap::from([(-1, voters.clone())]),
            version_history: BTreeMap::from([(-1, version)]),
            committed_voters: voters,
            committed_version: version,
        }
    }

    pub fn latest_voters(&self) -> &VoterSet {
        self.voter_history
            .last_key_value()
            .map_or(&self.committed_voters, |(_, voters)| voters)
    }

    pub fn latest_version(&self) -> u16 {
        self.version_history
            .last_key_value()
            .map_or(self.committed_version, |(_, version)| *version)
    }

    pub fn voters_at(&self, end_offset: Offset) -> VoterSet {
        self.voter_history
            .range(..end_offset.0)
            .next_back()
            .map_or_else(
                || self.committed_voters.clone(),
                |(_, voters)| voters.clone(),
            )
    }

    pub fn version_at(&self, end_offset: Offset) -> u16 {
        self.version_history
            .range(..end_offset.0)
            .next_back()
            .map_or(self.committed_version, |(_, version)| *version)
    }

    pub fn apply(&mut self, offset: i64, record: &ControlRecord) -> Result<(), RaftError> {
        match record {
            ControlRecord::KRaftVersion(record) => {
                let version = u16::try_from(record.k_raft_version).map_err(|_| {
                    RaftError::ChangeRejected("negative kraft.version control record".into())
                })?;
                self.version_history.insert(offset, version);
            }
            ControlRecord::Voters(record) => {
                self.voter_history
                    .insert(offset, voter_set_from_wire(record)?);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn truncate_to(&mut self, offset: i64) {
        self.voter_history
            .retain(|record_offset, _| *record_offset < offset);
        self.version_history
            .retain(|record_offset, _| *record_offset < offset);
    }

    pub fn commit_to(&mut self, high_watermark: i64) -> bool {
        let voters = self
            .voter_history
            .range(..high_watermark)
            .next_back()
            .map_or_else(
                || self.committed_voters.clone(),
                |(_, voters)| voters.clone(),
            );
        let version = self
            .version_history
            .range(..high_watermark)
            .next_back()
            .map_or(self.committed_version, |(_, version)| *version);
        let changed = voters != self.committed_voters || version != self.committed_version;
        self.committed_voters = voters;
        self.committed_version = version;
        changed
    }
}

pub fn voter_supports_version(voter: &krabka_metadata::voters::Voter, version: u16) -> bool {
    voter.kraft_version.min <= version && version <= voter.kraft_version.max
}

pub fn voter_set_to_wire(voters: &VoterSet) -> WireVotersRecord {
    let voters = voters
        .iter()
        .map(|voter| WireVoter {
            voter_id: i32::try_from(voter.id.0).unwrap_or(i32::MAX),
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
                min_supported_version: i16::try_from(voter.kraft_version.min).unwrap_or(i16::MAX),
                max_supported_version: i16::try_from(voter.kraft_version.max).unwrap_or(i16::MAX),
                ..Default::default()
            },
            ..Default::default()
        })
        .collect();
    WireVotersRecord {
        version: 0,
        voters,
        ..Default::default()
    }
}

pub fn voter_set_from_wire(record: &WireVotersRecord) -> Result<VoterSet, RaftError> {
    let voters = record
        .voters
        .iter()
        .map(|voter| {
            let id = u64::try_from(voter.voter_id).map_err(|_| {
                RaftError::InvalidVoterUpdate("negative voter id in VotersRecord".into())
            })?;
            let min = u16::try_from(voter.k_raft_version_feature.min_supported_version).map_err(
                |_| RaftError::InvalidVoterUpdate("negative minimum kraft.version".into()),
            )?;
            let max = u16::try_from(voter.k_raft_version_feature.max_supported_version).map_err(
                |_| RaftError::InvalidVoterUpdate("negative maximum kraft.version".into()),
            )?;
            if min > max {
                return Err(RaftError::InvalidVoterUpdate(
                    "inverted kraft.version range".into(),
                ));
            }
            let endpoints = voter
                .endpoints
                .iter()
                .map(|endpoint| {
                    let port = endpoint.port;
                    if endpoint.name.is_empty() || endpoint.host.is_empty() || port == 0 {
                        return Err(RaftError::InvalidVoterUpdate(
                            "voter endpoint must have a name, host, and nonzero port".into(),
                        ));
                    }
                    Ok(krabka_metadata::voters::VoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port,
                    })
                })
                .collect::<Result<Vec<_>, RaftError>>()?;
            Ok(krabka_metadata::voters::Voter {
                id: NodeId(id),
                directory_id: Uuid::from_bytes(voter.voter_directory_id.0),
                endpoints,
                kraft_version: krabka_metadata::voters::KRaftVersionRange { min, max },
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    let set = VoterSet::from_voters(voters);
    if set.len() != record.voters.len() {
        return Err(RaftError::InvalidVoterUpdate(
            "duplicate voter id in VotersRecord".into(),
        ));
    }
    Ok(set)
}
