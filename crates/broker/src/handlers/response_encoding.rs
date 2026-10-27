//! Shared wire-body decoding and response encoding.
//!
//! It sizes the buffer from `encoded_len` before it encodes, so the encode
//! writes into a buffer that already holds the whole response.

use {
    super::wire_types::ApiVersion,
    crate::{codes::NONE, error::BrokerError},
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode,
        krabka::barrier::ListBarrierCutsResponse,
        owned::{
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            add_raft_voter_response::AddRaftVoterResponse, alter_client_quotas_response::EntryData,
            alter_configs_response::AlterConfigsResourceResponse,
            alter_partition_reassignments_response::ReassignablePartitionResponse,
            alter_share_group_offsets_response::AlterShareGroupOffsetsResponse,
            alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
            broker_heartbeat_response::BrokerHeartbeatResponse,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
            delete_acls_response::DeleteAclsFilterResult,
            delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse,
            describe_acls_response::DescribeAclsResponse,
            describe_delegation_token_response::DescribeDelegationTokenResponse,
            describe_quorum_response::DescribeQuorumResponse,
            elect_leaders_response::PartitionResult,
            expire_delegation_token_response::ExpireDelegationTokenResponse,
            heartbeat_response::HeartbeatResponse, incremental_alter_configs_response,
            leave_group_response::LeaveGroupResponse, offset_delete_response::OffsetDeleteResponse,
            remove_raft_voter_response::RemoveRaftVoterResponse,
            renew_delegation_token_response::RenewDelegationTokenResponse,
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_response::ShareFetchResponse,
            share_group_heartbeat_response::ShareGroupHeartbeatResponse,
            streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
            sync_group_response::SyncGroupResponse,
            unregister_broker_response::UnregisterBrokerResponse,
            update_raft_voter_response::UpdateRaftVoterResponse,
        },
    },
};

/// Decode one request body at the supplied version. As at the wire entry
/// points, bytes beyond that body are left unread.
pub fn decode_request<'a, R: Decode<'a>>(
    bytes: &'a [u8],
    version: ApiVersion,
) -> Result<R, BrokerError> {
    let mut cursor = bytes;
    Ok(R::decode(&mut cursor, version)?)
}

pub fn encode_response<R: Encode>(resp: &R, version: ApiVersion) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// A response whose whole-request refusal is its top-level `error_code` and
/// `error_message`, with every other field at its generated default.
pub trait ErrorResponse: Default {
    fn error(error_code: i16, error_message: Option<String>) -> Self;
}

/// An [`ErrorResponse`] for a response that has no top-level `error_message`.
pub trait ErrorCodeResponse: Default {
    fn error(error_code: i16) -> Self;
}

/// A per-item response row that carries its own `error_code` and
/// `error_message`.
pub trait ErrorRow {
    fn error_code(&self) -> i16;
    fn set_error(&mut self, error_code: i16, error_message: Option<String>);

    fn with_error(mut self, error_code: i16, error_message: String) -> Self
    where
        Self: Sized,
    {
        self.set_error(error_code, Some(error_message));
        self
    }
}

/// A generated per-resource error row from either config-alter API.
pub trait ResourceErrorRow: ErrorRow + Default {
    fn for_resource(resource_type: i8, resource_name: String) -> Self;
}

/// Stamps `error_code` and `error_message` on every row still at `NONE`, and
/// leaves a row an earlier check already refused as it is: the rewrite a
/// handler applies when the metadata submit behind its accepted rows fails.
pub fn stamp_unset<'a, R: ErrorRow + 'a>(
    rows: impl IntoIterator<Item = &'a mut R>,
    error_code: i16,
    error_message: &str,
) {
    for row in rows {
        if row.error_code() == NONE {
            row.set_error(error_code, Some(error_message.to_owned()));
        }
    }
}

krabka_macros::response_errors! {
    response {
        ListBarrierCutsResponse,
        AddRaftVoterResponse,
        AlterShareGroupOffsetsResponse,
        ConsumerGroupHeartbeatResponse,
        DeleteShareGroupOffsetsResponse,
        DescribeAclsResponse,
        DescribeQuorumResponse,
        RemoveRaftVoterResponse,
        // Kafka's `getErrorResponse` for both share RPCs leaves the acquisition
        // lock timeout at 0, not the configured one.
        ShareAcknowledgeResponse,
        ShareFetchResponse,
        ShareGroupHeartbeatResponse,
        StreamsGroupTopologyDescriptionUpdateResponse,
        UnregisterBrokerResponse,
    }
    code_response {
        AddOffsetsToTxnResponse,
        BrokerHeartbeatResponse,
        DescribeDelegationTokenResponse,
        ExpireDelegationTokenResponse,
        HeartbeatResponse,
        LeaveGroupResponse,
        OffsetDeleteResponse,
        RenewDelegationTokenResponse,
        SyncGroupResponse,
        UpdateRaftVoterResponse,
    }
    row {
        EntryData,
        ReassignablePartitionResponse,
        AlterUserScramCredentialsResult,
        DeleteAclsFilterResult,
        PartitionResult,
    }
    resource_row {
        AlterConfigsResourceResponse,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            codes::{NONE, NOT_CONTROLLER},
        },
        assert2::assert,
        krabka_protocol::{
            Decode,
            owned::api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };

    #[test]
    fn encode_response_round_trips_protocol_body() {
        let resp = ApiVersionsResponse {
            error_code: NONE,
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

        assert!(decoded.error_code == codes::NONE);
        assert!(decoded.api_keys.len() == 1);
        assert!(decoded.api_keys[0].api_key == 18);
        assert!(decoded.api_keys[0].min_version == 0);
        assert!(decoded.api_keys[0].max_version == 4);
    }

    #[test]
    fn stamp_unset_rewrites_only_rows_still_at_none() {
        use krabka_protocol::owned::alter_partition_reassignments_response::ReassignablePartitionResponse;

        let row = |partition_index, error_code, error_message: Option<&str>| {
            ReassignablePartitionResponse {
                partition_index,
                error_code,
                error_message: error_message.map(str::to_owned),
                ..Default::default()
            }
        };
        let mut rows = vec![
            row(7, codes::NONE, None),
            row(
                8,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("unknown partition"),
            ),
        ];

        stamp_unset(&mut rows, NOT_CONTROLLER, "submit failed: not controller");

        let expected = vec![
            row(
                7,
                codes::NOT_CONTROLLER,
                Some("submit failed: not controller"),
            ),
            row(
                8,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("unknown partition"),
            ),
        ];
        assert!(rows == expected);
    }
}
