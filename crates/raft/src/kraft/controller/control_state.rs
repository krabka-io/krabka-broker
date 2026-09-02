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

fn history_value_before<T>(history: &BTreeMap<i64, T>, frontier: i64) -> Option<&T> {
    let offsets: Vec<i64> = history.keys().copied().collect();
    let prefix_len = krabka_verified::raft::control_history_frontier(&offsets, frontier);
    if prefix_len == 0 {
        None
    } else {
        history.get(&offsets[prefix_len - 1])
    }
}

fn truncate_history<T>(history: &mut BTreeMap<i64, T>, frontier: i64) {
    let offsets: Vec<i64> = history.keys().copied().collect();
    let prefix_len = krabka_verified::raft::control_history_frontier(&offsets, frontier);
    if prefix_len < offsets.len() {
        drop(history.split_off(&offsets[prefix_len]));
    }
}

impl KraftControlState {
    pub fn new(voters: VoterSet, version: u16) -> Self {
        Self {
            voter_history: maplit::btreemap! {-1 => voters.clone()},
            version_history: maplit::btreemap! {-1 => version},
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
        history_value_before(&self.voter_history, end_offset.0)
            .map_or_else(|| self.committed_voters.clone(), Clone::clone)
    }

    pub fn version_at(&self, end_offset: Offset) -> u16 {
        history_value_before(&self.version_history, end_offset.0)
            .map_or(self.committed_version, |version| *version)
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
        truncate_history(&mut self.voter_history, offset);
        truncate_history(&mut self.version_history, offset);
    }

    pub fn commit_to(&mut self, high_watermark: i64) -> bool {
        let voters = history_value_before(&self.voter_history, high_watermark)
            .map_or_else(|| self.committed_voters.clone(), Clone::clone);
        let version = history_value_before(&self.version_history, high_watermark)
            .map_or(self.committed_version, |version| *version);
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
    let mut voter_ids = std::collections::BTreeSet::new();
    let voter_ids_unique = record
        .voters
        .iter()
        .all(|voter| voter_ids.insert(voter.voter_id));
    match krabka_verified::voter_set_wire_decision(
        record.version,
        record.voters.len(),
        voter_ids_unique,
    ) {
        krabka_verified::VoterSetWireDecision::UnsupportedRecordVersion => {
            return Err(RaftError::InvalidVoterUpdate(format!(
                "unsupported VotersRecord version {}",
                record.version
            )));
        }
        krabka_verified::VoterSetWireDecision::Empty => {
            return Err(RaftError::InvalidVoterUpdate(
                "empty voter set in VotersRecord".into(),
            ));
        }
        krabka_verified::VoterSetWireDecision::DuplicateId => {
            return Err(RaftError::InvalidVoterUpdate(
                "duplicate voter id in VotersRecord".into(),
            ));
        }
        krabka_verified::VoterSetWireDecision::Accept => {}
    }
    let voters = record
        .voters
        .iter()
        .map(|voter| {
            let directory_id = Uuid::from_bytes(voter.voter_directory_id.0);
            let directory_id_exact = directory_id.as_bytes() == &voter.voter_directory_id.0;
            let mut endpoint_names = std::collections::BTreeSet::new();
            let endpoints_valid = voter.endpoints.iter().all(|endpoint| {
                !endpoint.name.is_empty()
                    && !endpoint.host.is_empty()
                    && endpoint.port != 0
                    && endpoint_names.insert(endpoint.name.as_str())
            });
            match krabka_verified::voter_wire_decision(
                voter.voter_id,
                directory_id_exact,
                voter.endpoints.len(),
                endpoints_valid,
                voter.k_raft_version_feature.min_supported_version,
                voter.k_raft_version_feature.max_supported_version,
            ) {
                krabka_verified::VoterWireDecision::NegativeId => {
                    return Err(RaftError::InvalidVoterUpdate(
                        "negative voter id in VotersRecord".into(),
                    ));
                }
                krabka_verified::VoterWireDecision::DirectoryMismatch => {
                    return Err(RaftError::InvalidVoterUpdate(
                        "voter directory id changed during decoding".into(),
                    ));
                }
                krabka_verified::VoterWireDecision::InvalidEndpoint => {
                    return Err(RaftError::InvalidVoterUpdate(
                        "voter endpoints must be nonempty, uniquely named, and have a name, host, and nonzero port".into(),
                    ));
                }
                krabka_verified::VoterWireDecision::InvalidVersionRange => {
                    return Err(RaftError::InvalidVoterUpdate(
                        "invalid kraft.version range in VotersRecord".into(),
                    ));
                }
                krabka_verified::VoterWireDecision::Accept => {}
            }
            let id = u64::try_from(voter.voter_id)
                .expect("voter wire admission proves the voter id is nonnegative");
            let min = u16::try_from(voter.k_raft_version_feature.min_supported_version)
                .expect("voter wire admission proves the minimum version is nonnegative");
            let max = u16::try_from(voter.k_raft_version_feature.max_supported_version)
                .expect("voter wire admission proves the maximum version is nonnegative");
            let endpoints = voter
                .endpoints
                .iter()
                .map(|endpoint| {
                    krabka_metadata::voters::VoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                    }
                })
                .collect();
            Ok(krabka_metadata::voters::Voter {
                id: NodeId(id),
                directory_id,
                endpoints,
                kraft_version: krabka_metadata::voters::KRaftVersionRange { min, max },
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    Ok(VoterSet::from_voters(voters))
}
