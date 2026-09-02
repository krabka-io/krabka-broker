//! The node-local durable quorum-state file. Kafka's `QuorumStateData` JSON is
//! read by the JVM tools, so both schema versions keep their exact field set
//! and Kafka field order here.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use uuid::Uuid;

use super::QUORUM_STATE_FILE;
use crate::{
    error::RaftError,
    kraft::types::{NodeId, QuorumState, ReplicaKey},
};

/// One entry of schema v0's `currentVoters` array.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuorumStateVoter {
    #[serde(rename = "voterId")]
    voter_id: i32,
}

/// Kafka's on-disk `QuorumStateData`. Schema v0 carries the cluster id, the
/// applied offset and the legacy voter ids; schema v1 carries the voted
/// directory id instead. The `skip_serializing_if` markers keep each version's
/// field set (and their Kafka field order) byte-exact.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuorumStateJson {
    #[serde(rename = "clusterId", skip_serializing_if = "Option::is_none")]
    cluster_id: Option<String>,
    #[serde(rename = "leaderId")]
    leader_id: i32,
    #[serde(rename = "leaderEpoch")]
    leader_epoch: i32,
    #[serde(rename = "votedId")]
    voted_id: i32,
    #[serde(rename = "votedDirectoryId", skip_serializing_if = "Option::is_none")]
    voted_directory_id: Option<String>,
    #[serde(rename = "appliedOffset", skip_serializing_if = "Option::is_none")]
    applied_offset: Option<i64>,
    #[serde(rename = "currentVoters", skip_serializing_if = "Option::is_none")]
    current_voters: Option<Vec<QuorumStateVoter>>,
    data_version: i32,
}

/// Write Kafka's JSON `QuorumStateData` atomically. Level 0 uses schema v0
/// (including the legacy voter ids); level 1 uses schema v1 with the voted
/// directory id and no embedded voter set.
pub fn save_quorum_state(dir: &std::path::Path, state: &QuorumState) -> Result<(), RaftError> {
    use std::io::Write as _;

    let voter_ids = state.voters.ids();
    let all_voter_ids_fit = voter_ids.iter().all(|id| i32::try_from(id.0).is_ok());
    let leader_id_value = state.leader_id.map_or(0, |id| id.0);
    let voted_id_value = state.voted_key.map_or(0, |key| key.id.0);
    if krabka_verified::quorum_state_write_decision(
        state.kraft_version,
        state.leader_epoch,
        state.leader_id.is_some(),
        leader_id_value,
        state.voted_key.is_some(),
        voted_id_value,
        all_voter_ids_fit,
    ) == krabka_verified::QuorumStateWriteDecision::Reject
    {
        return Err(RaftError::Storage(krabka_log::LogError::InvalidArgument(
            "quorum-state cannot be represented exactly by schema v0 or v1".into(),
        )));
    }
    let leader_id = state
        .leader_id
        .map_or(Ok(-1), |id| i32::try_from(id.0))
        .expect("write admission proves the leader id fits i32");
    let leader_epoch =
        i32::try_from(state.leader_epoch).expect("write admission proves the epoch fits i32");
    let voted_id = state
        .voted_key
        .map_or(Ok(-1), |key| i32::try_from(key.id.0))
        .expect("write admission proves the voted id fits i32");
    let data = if state.kraft_version == 0 {
        QuorumStateJson {
            cluster_id: Some(state.cluster_id.to_string()),
            leader_id,
            leader_epoch,
            voted_id,
            voted_directory_id: None,
            applied_offset: Some(0),
            current_voters: Some(
                voter_ids
                    .into_iter()
                    .map(|id| QuorumStateVoter {
                        voter_id: i32::try_from(id.0)
                            .expect("write admission proves every v0 voter id fits i32"),
                    })
                    .collect(),
            ),
            data_version: 0,
        }
    } else {
        let voted_directory_id = state
            .voted_key
            .map_or([0; 16], |key| *key.directory_id.as_bytes());
        QuorumStateJson {
            cluster_id: None,
            leader_id,
            leader_epoch,
            voted_id,
            voted_directory_id: Some(URL_SAFE_NO_PAD.encode(voted_directory_id)),
            applied_offset: None,
            current_voters: None,
            data_version: 1,
        }
    };
    let json = serde_json::to_string(&data).map_err(|e| {
        RaftError::Storage(krabka_log::LogError::Corrupt(format!(
            "serialize quorum-state: {e}"
        )))
    })?;
    let path = dir.join(QUORUM_STATE_FILE);
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp).map_err(krabka_log::LogError::Io)?;
    file.write_all(json.as_bytes())
        .map_err(krabka_log::LogError::Io)?;
    file.sync_all().map_err(krabka_log::LogError::Io)?;
    std::fs::rename(&tmp, &path).map_err(krabka_log::LogError::Io)?;
    Ok(())
}

/// Load Kafka JSON `QuorumStateData`. The configured voter metadata supplies
/// endpoints at level 0; level-1 membership is recovered from snapshots/log.
pub fn load_quorum_state(
    dir: &std::path::Path,
    cluster_id: Uuid,
    voters: &krabka_metadata::voters::VoterSet,
) -> Result<Option<QuorumState>, RaftError> {
    let path = dir.join(QUORUM_STATE_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(RaftError::Storage(krabka_log::LogError::Io(e))),
    };
    let Ok(data) = serde_json::from_slice::<QuorumStateJson>(&bytes) else {
        return Ok(None);
    };
    let data_version = data.data_version;
    let voted_directory_id = data
        .voted_directory_id
        .as_deref()
        .and_then(|id| URL_SAFE_NO_PAD.decode(id).ok())
        .and_then(|raw| <[u8; 16]>::try_from(raw).ok())
        .map(Uuid::from_bytes);
    let configured_voter_ids = voters.ids();
    let v0_voters_match = data.current_voters.as_ref().is_some_and(|persisted| {
        persisted.len() == configured_voter_ids.len()
            && persisted
                .iter()
                .zip(&configured_voter_ids)
                .all(|(stored, configured)| {
                    stored.voter_id >= 0
                        && u64::try_from(stored.voter_id).ok() == Some(configured.0)
                })
    });
    let version_fields_valid = if data_version == 0 {
        data.cluster_id.as_deref() == Some(cluster_id.to_string().as_str())
            && data.voted_directory_id.is_none()
            && data.applied_offset == Some(0)
            && v0_voters_match
    } else if data_version == 1 {
        data.cluster_id.is_none()
            && data.voted_directory_id.is_some()
            && data.applied_offset.is_none()
            && data.current_voters.is_none()
    } else {
        false
    };
    let decision = krabka_verified::quorum_state_load_decision(
        data_version,
        data.leader_epoch,
        data.voted_id,
        version_fields_valid,
        voted_directory_id.is_some(),
        voted_directory_id == Some(Uuid::nil()),
    );
    let voted_key = match decision {
        krabka_verified::QuorumStateLoadDecision::Reject => return Ok(None),
        krabka_verified::QuorumStateLoadDecision::RestoreNoVote => None,
        krabka_verified::QuorumStateLoadDecision::RestoreVote => Some(ReplicaKey {
            id: NodeId(
                u64::try_from(data.voted_id)
                    .expect("load admission proves the voted id is nonnegative"),
            ),
            directory_id: if data_version == 1 {
                voted_directory_id.expect("load admission proves the v1 directory id is valid")
            } else {
                Uuid::nil()
            },
        }),
    };
    let leader_epoch =
        u32::try_from(data.leader_epoch).expect("load admission proves the epoch is nonnegative");
    // Leadership is VOLATILE, not durable: Raft persists only currentTerm
    // (`leader_epoch`) and votedFor (`voted_key`), never the current leader. A
    // restarted node must NOT trust a persisted `leader_id` — especially an
    // ex-leader, which would otherwise come back believing it is still the
    // leader (stale `leader_id == self`), publish itself via `watch_leader`,
    // and never re-discover the real leader elected while it was down. Start
    // with no known leader; the node re-attaches via the current leader's
    // `BeginQuorumEpoch` heartbeat (higher epoch → Follower) or a re-election.
    Ok(Some(QuorumState {
        cluster_id,
        leader_epoch,
        leader_id: None,
        voted_key,
        voters: voters.clone(),
        kraft_version: u16::try_from(data_version)
            .expect("load admission proves the schema version is zero or one"),
    }))
}
