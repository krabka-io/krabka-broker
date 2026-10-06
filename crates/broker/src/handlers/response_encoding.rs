//! The response encoders that every handler ends with.
//!
//! Both size the buffer from `encoded_len` before they encode, so the encode
//! writes into a buffer that already holds the whole response.

use bytes::{Bytes, BytesMut};
use krabka_protocol::Encode;

use super::wire_types::ApiVersion;
use crate::error::BrokerError;

pub(crate) fn encode_response<R: Encode>(
    resp: &R,
    version: ApiVersion,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

pub(crate) fn encode_response_with_context<R: Encode>(
    resp: &R,
    version: ApiVersion,
    context: &'static str,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)
        .map_err(|e| BrokerError::Replication(format!("{context}: {e}")))?;
    Ok(buf.freeze())
}

/// A response whose whole-request refusal is its top-level `error_code` and
/// `error_message`, with every other field at its generated default.
pub(crate) trait ErrorResponse: Default {
    fn error(error_code: i16, error_message: Option<String>) -> Self;
}

/// An [`ErrorResponse`] for a response that has no top-level `error_message`.
pub(crate) trait ErrorCodeResponse: Default {
    fn error(error_code: i16) -> Self;
}

macro_rules! impl_error_response {
    (code_only: $($ty:path),* $(,)?) => {
        $(impl ErrorCodeResponse for $ty {
            fn error(error_code: i16) -> Self {
                Self {
                    error_code,
                    ..Self::default()
                }
            }
        })*
    };
    ($($ty:path),* $(,)?) => {
        $(impl ErrorResponse for $ty {
            fn error(error_code: i16, error_message: Option<String>) -> Self {
                Self {
                    error_code,
                    error_message,
                    ..Self::default()
                }
            }
        })*
    };
}

impl_error_response!(
    krabka_protocol::krabka::barrier::ListBarrierCutsResponse,
    krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse,
    krabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    krabka_protocol::owned::describe_acls_response::DescribeAclsResponse,
    krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse,
    krabka_protocol::owned::remove_raft_voter_response::RemoveRaftVoterResponse,
    krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    krabka_protocol::owned::streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
    krabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse,
);

impl_error_response!(
    code_only:
    krabka_protocol::owned::add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    krabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse,
    krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
    krabka_protocol::owned::expire_delegation_token_response::ExpireDelegationTokenResponse,
    krabka_protocol::owned::leave_group_response::LeaveGroupResponse,
    krabka_protocol::owned::offset_delete_response::OffsetDeleteResponse,
    krabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse,
    krabka_protocol::owned::sync_group_response::SyncGroupResponse,
    krabka_protocol::owned::update_raft_voter_response::UpdateRaftVoterResponse,
);

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        Decode,
        owned::api_versions_response::{ApiVersion, ApiVersionsResponse},
    };

    use super::*;

    #[test]
    fn encode_response_round_trips_protocol_body() {
        let resp = ApiVersionsResponse {
            error_code: crate::codes::NONE,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            }],
            throttle_time_ms: 0,
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        let mut cur: &[u8] = &bytes;
        let decoded = ApiVersionsResponse::decode(&mut cur, 3).expect("decode response");

        assert!(decoded.error_code == crate::codes::NONE);
        assert!(decoded.api_keys.len() == 1);
        assert!(decoded.api_keys[0].api_key == 18);
        assert!(decoded.api_keys[0].min_version == 0);
        assert!(decoded.api_keys[0].max_version == 4);
    }
}
