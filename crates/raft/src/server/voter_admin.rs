//! The KIP-853 voter-reconfiguration handlers: `AddRaftVoter`,
//! `RemoveRaftVoter` and `UpdateRaftVoter`, together with the request
//! validation, the candidate probe and the translation of a reconfiguration
//! outcome into a Kafka error code that all three share.

use bytes::{Bytes, BytesMut};

use crate::{error::RaftError, kraft::KraftController};

#[cfg(test)]
mod tests;

fn reconfiguration_error_code(
    result: Result<crate::reconfig::ReconfigOutcome, RaftError>,
) -> (i16, Option<String>) {
    use crate::reconfig::ReconfigOutcome;
    match result {
        Ok(ReconfigOutcome::Committed) => (0, None),
        Ok(ReconfigOutcome::NotLeader { leader }) => (
            6,
            Some(leader.map_or_else(
                || "not the raft leader".into(),
                |leader| format!("not the raft leader; current leader is {leader}"),
            )),
        ),
        Err(RaftError::ReconfigInProgress) => {
            (7, Some("another reconfiguration is in progress".into()))
        }
        Err(RaftError::VoterNotCaughtUp { id, lag }) => {
            (42, Some(format!("voter {id} not caught up (lag {lag})")))
        }
        Err(RaftError::DuplicateVoter(id)) => (139, Some(format!("voter {id} already exists"))),
        Err(RaftError::VoterNotFound(id)) => (140, Some(format!("voter {id} was not found"))),
        Err(RaftError::InvalidVoterUpdate(message)) => (141, Some(message)),
        Err(RaftError::UnsupportedKraftVersion(_)) => (
            35,
            Some("dynamic voter changes require kraft.version 1".into()),
        ),
        Err(RaftError::ReconfigRejected(message)) => (42, Some(message)),
        Err(error) => (-1, Some(error.to_string())),
    }
}

fn valid_wire_listeners<'a>(listeners: impl IntoIterator<Item = (&'a str, &'a str, u16)>) -> bool {
    let mut names = std::collections::BTreeSet::new();
    let mut count = 0usize;
    for (name, host, port) in listeners {
        count += 1;
        if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name) {
            return false;
        }
    }
    count != 0
}

async fn probe_voter_candidate(
    listeners: &[krabka_protocol::owned::add_raft_voter_request::Listener],
    finalized_version: u16,
    engine: &KraftController,
) -> Result<(), (i16, String)> {
    let endpoint = listeners
        .iter()
        .find(|listener| listener.name.eq_ignore_ascii_case("CONTROLLER"))
        .or_else(|| listeners.first())
        .expect("validated non-empty listeners");
    let address = format!("{}:{}", endpoint.host, endpoint.port);
    let supported = engine
        .probe_kraft_version(&address, finalized_version)
        .await
        .map_err(|error| (7, format!("candidate ApiVersions probe failed: {error}")))?;
    if supported {
        Ok(())
    } else {
        Err((
            35,
            format!("candidate does not support finalized kraft.version {finalized_version}"),
        ))
    }
}

pub(super) async fn add_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request::AddRaftVoterRequest,
            add_raft_voter_response::AddRaftVoterResponse,
        },
    };

    let mut input = body;
    let request = AddRaftVoterRequest::decode(&mut input, version)?;
    let image = engine.current_image();
    let cluster_id = image.cluster_id().to_string();
    let valid = request
        .cluster_id
        .as_deref()
        .is_none_or(|request_cluster| request_cluster == cluster_id)
        && request.voter_id >= 0
        && request.voter_directory_id != krabka_protocol::primitives::uuid::Uuid::ZERO
        && valid_wire_listeners(request.listeners.iter().map(|listener| {
            (
                listener.name.as_str(),
                listener.host.as_str(),
                listener.port,
            )
        }));
    let probe = if valid && image.kraft_version() >= 1 {
        probe_voter_candidate(&request.listeners, image.kraft_version(), engine).await
    } else {
        Ok(())
    };
    let (error_code, error_message) = if !valid {
        (42, Some("invalid AddRaftVoter request".into()))
    } else if let Err((code, message)) = probe {
        (code, Some(message))
    } else {
        let voter = krabka_metadata::Voter {
            id: crate::NodeId(u64::try_from(request.voter_id).unwrap_or_default()),
            directory_id: uuid::Uuid::from_bytes(request.voter_directory_id.0),
            endpoints: request
                .listeners
                .into_iter()
                .map(|listener| krabka_metadata::VoterEndpoint {
                    name: listener.name,
                    host: listener.host,
                    port: listener.port,
                })
                .collect(),
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        };
        reconfiguration_error_code(
            engine
                .reconfigure(crate::reconfig::VoterChange::Add(
                    crate::reconfig::AddVoter {
                        voter,
                        ack_when_committed: version == 0 || request.ack_when_committed,
                    },
                ))
                .await,
        )
    };
    let mut output = BytesMut::new();
    AddRaftVoterResponse {
        error_code,
        error_message,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}

pub(super) async fn remove_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            remove_raft_voter_request::RemoveRaftVoterRequest,
            remove_raft_voter_response::RemoveRaftVoterResponse,
        },
    };

    let mut input = body;
    let request = RemoveRaftVoterRequest::decode(&mut input, version)?;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let validation_error = if request
        .cluster_id
        .as_deref()
        .is_some_and(|request_cluster| request_cluster != cluster_id)
    {
        Some(format!(
            "cluster_id {:?} does not match {cluster_id}",
            request.cluster_id
        ))
    } else if request.voter_id < 0 {
        Some(format!(
            "voter_id must be non-negative, got {}",
            request.voter_id
        ))
    } else if request.voter_directory_id == krabka_protocol::primitives::uuid::Uuid::ZERO {
        Some("voter_directory_id must be non-zero".into())
    } else {
        None
    };
    let (error_code, error_message) = if let Some(message) = validation_error {
        (42, Some(message))
    } else {
        reconfiguration_error_code(
            engine
                .reconfigure(crate::reconfig::VoterChange::Remove(
                    crate::reconfig::RemoveVoter {
                        id: crate::NodeId(u64::try_from(request.voter_id).unwrap_or_default()),
                        directory_id: uuid::Uuid::from_bytes(request.voter_directory_id.0),
                    },
                ))
                .await,
        )
    };
    let mut output = BytesMut::new();
    RemoveRaftVoterResponse {
        error_code,
        error_message,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}

pub(super) async fn update_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            update_raft_voter_request::UpdateRaftVoterRequest,
            update_raft_voter_response::UpdateRaftVoterResponse,
        },
    };

    let mut input = body;
    let request = UpdateRaftVoterRequest::decode(&mut input, version)?;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let quorum = engine.quorum_state().await?;
    let min = u16::try_from(request.k_raft_version_feature.min_supported_version);
    let max = u16::try_from(request.k_raft_version_feature.max_supported_version);
    let valid_range = matches!((&min, &max), (Ok(min), Ok(max)) if min <= max);
    let valid = request.cluster_id.as_deref() == Some(cluster_id.as_str())
        && request.voter_id >= 0
        && request.voter_directory_id != krabka_protocol::primitives::uuid::Uuid::ZERO
        && i64::from(request.current_leader_epoch) == i64::from(quorum.leader_epoch)
        && valid_range
        && valid_wire_listeners(request.listeners.iter().map(|listener| {
            (
                listener.name.as_str(),
                listener.host.as_str(),
                listener.port,
            )
        }));
    let error_code = if valid {
        let voter = krabka_metadata::Voter {
            id: crate::NodeId(u64::try_from(request.voter_id).unwrap_or_default()),
            directory_id: uuid::Uuid::from_bytes(request.voter_directory_id.0),
            endpoints: request
                .listeners
                .into_iter()
                .map(|listener| krabka_metadata::VoterEndpoint {
                    name: listener.name,
                    host: listener.host,
                    port: listener.port,
                })
                .collect(),
            kraft_version: krabka_metadata::KRaftVersionRange {
                min: min.unwrap_or_default(),
                max: max.unwrap_or_default(),
            },
        };
        reconfiguration_error_code(
            engine
                .reconfigure(crate::reconfig::VoterChange::Update(
                    crate::reconfig::UpdateVoter { voter },
                ))
                .await,
        )
        .0
    } else {
        141
    };
    let mut output = BytesMut::new();
    UpdateRaftVoterResponse {
        error_code,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}
