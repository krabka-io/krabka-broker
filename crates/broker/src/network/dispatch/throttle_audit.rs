//! KIP-219 throttle-echo audit.
//!
//! [`super::response::throttle_is_leading_field`] decides whether the dispatch
//! loop may report a request-quota delay by patching the first int32 of an
//! already-encoded response body. Getting that table wrong is silent: an API
//! that is missing from it answers `throttle_time_ms = 0` while the broker
//! sleeps on the connection, so a client never backs off and the quota
//! degrades into latency injection.
//!
//! This module pins the table against the generated encoders. For every
//! `(api_key, version)` pair [`crate::api_catalog::supported_apis`] advertises
//! it encodes that API's response with a sentinel in `throttle_time_ms` and
//! looks at where the sentinel lands, which is the byte layout the pinned
//! `krabka-protocol` response schemas produce rather than a restatement of the
//! table under test. An API added to the catalog but not to [`probes`] fails
//! [`every_advertised_api_is_classified`].

use std::collections::BTreeMap;

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::Encode;

use super::response::throttle_is_leading_field;
use crate::{
    api_catalog::supported_apis,
    handlers::{ApiKeyCode, ApiVersion},
};

/// Written into `throttle_time_ms` before encoding. Any value works as long as
/// it is not a field default, so that finding it at offset 0 means the encoder
/// really put `ThrottleTimeMs` first.
const SENTINEL: i32 = 0x5EED_0219;

/// Where `ThrottleTimeMs` sits in an encoded response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThrottlePosition {
    /// First field: the dispatch loop can patch it in place.
    Leading,
    /// Present, but behind at least one other field. A leading patch would
    /// corrupt the response, so the delay is applied without being echoed.
    Buried,
    /// The schema has no `ThrottleTimeMs` at this version.
    Absent,
}

/// `(api_key, first version, last version)` ranges the broker throttles
/// without echoing, because `ThrottleTimeMs` is not the leading field.
///
/// * `Produce` (0) and `ApiVersions` (18) carry it after a variable-length
///   array, so its offset is not knowable from the header alone. Produce is
///   moot in practice: its bandwidth quota is charged by the handler, which
///   fills the field in before encoding.
/// * The delegation-token APIs (38-41) carry it as the last field, behind
///   principal strings, timestamps, the token list or the HMAC.
/// * `OffsetDelete` (47) leads with `ErrorCode`, an int16 that a leading int32
///   patch would overwrite. Its throttle is the one that sits at a fixed
///   offset, so it alone could be patched by a second, per-API offset table.
///   That is not worth a second patching mode for one API: the fix that
///   covers all seven is to set the field on the typed response before
///   encoding, the way the Produce and Fetch handlers already do.
///
/// Mirrored as rows in the generated `docs/KIP_MATRIX.md`, which is
/// regenerated in CI from this constant.
const THROTTLE_ECHO_DIVERGENCES: &[(ApiKeyCode, ApiVersion, ApiVersion)] = &[
    (0, 1, 13), // Produce
    (18, 1, 5), // ApiVersions
    (38, 1, 3), // CreateDelegationToken
    (39, 1, 2), // RenewDelegationToken
    (40, 1, 2), // ExpireDelegationToken
    (41, 1, 3), // DescribeDelegationToken
    (47, 0, 0), // OffsetDelete
];

/// Encodes `response` at `version` and reports where `ThrottleTimeMs` landed.
///
/// `default_json` is the generated per-version field map for the same schema;
/// it distinguishes [`ThrottlePosition::Buried`] from
/// [`ThrottlePosition::Absent`] once the sentinel is known not to lead. A
/// version the type cannot encode -- Produce below v3 and Fetch below v4 route
/// to the `kafka_3_6_2` flavors, and `ListOffsets` v0 is hand-rolled -- has no
/// leading throttle by construction.
fn position<R: Encode>(
    response: &R,
    version: ApiVersion,
    default_json: &serde_json::Value,
) -> ThrottlePosition {
    let mut body = BytesMut::new();
    if response.encode(&mut body, version).is_ok()
        && body.len() >= 4
        && body[..4] == SENTINEL.to_be_bytes()
    {
        return ThrottlePosition::Leading;
    }
    if default_json.get("throttleTimeMs").is_some() {
        ThrottlePosition::Buried
    } else {
        ThrottlePosition::Absent
    }
}

type Probe = fn(ApiVersion) -> ThrottlePosition;

/// An API whose response type has a `throttle_time_ms` field.
macro_rules! probe {
    ($module:ident, $response:ident) => {
        (krabka_protocol::owned::$module::API_KEY, {
            fn probe(version: ApiVersion) -> ThrottlePosition {
                use krabka_protocol::owned::$module as schema;

                let response = schema::$response {
                    throttle_time_ms: SENTINEL,
                    ..Default::default()
                };
                position(&response, version, &schema::default_json(version))
            }
            probe as Probe
        })
    };
}

/// An API whose response schema has no `ThrottleTimeMs` at any version. The
/// arm cannot set the sentinel, so the probe can only answer
/// [`ThrottlePosition::Absent`] or -- if a schema update grows the field --
/// [`ThrottlePosition::Buried`], which then fails the divergence test.
macro_rules! no_throttle_probe {
    ($module:ident, $response:ident) => {
        (krabka_protocol::owned::$module::API_KEY, {
            fn probe(version: ApiVersion) -> ThrottlePosition {
                use krabka_protocol::owned::$module as schema;

                let response = schema::$response::default();
                position(&response, version, &schema::default_json(version))
            }
            probe as Probe
        })
    };
}

/// Produce v0-v2 and Fetch v0-v3 are encoded from the `kafka_3_6_2` flavors,
/// mirroring `handlers::produce` and `handlers::fetch::encode_fetch_response`.
macro_rules! legacy_split_probe {
    ($module:ident, $response:ident, $canonical_from:literal) => {
        (krabka_protocol::owned::$module::API_KEY, {
            fn probe(version: ApiVersion) -> ThrottlePosition {
                use krabka_protocol::{
                    kafka_3_6_2::owned::$module as legacy, owned::$module as schema,
                };

                if version < $canonical_from {
                    let response = legacy::$response {
                        throttle_time_ms: SENTINEL,
                        ..Default::default()
                    };
                    position(&response, version, &legacy::default_json(version))
                } else {
                    let response = schema::$response {
                        throttle_time_ms: SENTINEL,
                        ..Default::default()
                    };
                    position(&response, version, &schema::default_json(version))
                }
            }
            probe as Probe
        })
    };
}

/// One probe per advertised `api_key`, keyed by the generated `API_KEY`
/// constant so an entry cannot drift onto the wrong API.
fn probes() -> BTreeMap<ApiKeyCode, Probe> {
    [
        legacy_split_probe!(produce_response, ProduceResponse, 3),
        legacy_split_probe!(fetch_response, FetchResponse, 4),
        probe!(list_offsets_response, ListOffsetsResponse),
        probe!(metadata_response, MetadataResponse),
        probe!(offset_commit_response, OffsetCommitResponse),
        probe!(offset_fetch_response, OffsetFetchResponse),
        probe!(find_coordinator_response, FindCoordinatorResponse),
        probe!(join_group_response, JoinGroupResponse),
        probe!(heartbeat_response, HeartbeatResponse),
        probe!(leave_group_response, LeaveGroupResponse),
        probe!(sync_group_response, SyncGroupResponse),
        probe!(describe_groups_response, DescribeGroupsResponse),
        probe!(list_groups_response, ListGroupsResponse),
        no_throttle_probe!(sasl_handshake_response, SaslHandshakeResponse),
        probe!(api_versions_response, ApiVersionsResponse),
        probe!(create_topics_response, CreateTopicsResponse),
        probe!(delete_topics_response, DeleteTopicsResponse),
        probe!(delete_records_response, DeleteRecordsResponse),
        probe!(init_producer_id_response, InitProducerIdResponse),
        probe!(
            offset_for_leader_epoch_response,
            OffsetForLeaderEpochResponse
        ),
        probe!(add_partitions_to_txn_response, AddPartitionsToTxnResponse),
        probe!(add_offsets_to_txn_response, AddOffsetsToTxnResponse),
        probe!(end_txn_response, EndTxnResponse),
        no_throttle_probe!(write_txn_markers_response, WriteTxnMarkersResponse),
        probe!(txn_offset_commit_response, TxnOffsetCommitResponse),
        probe!(describe_acls_response, DescribeAclsResponse),
        probe!(create_acls_response, CreateAclsResponse),
        probe!(delete_acls_response, DeleteAclsResponse),
        probe!(describe_configs_response, DescribeConfigsResponse),
        probe!(alter_configs_response, AlterConfigsResponse),
        probe!(alter_replica_log_dirs_response, AlterReplicaLogDirsResponse),
        probe!(describe_log_dirs_response, DescribeLogDirsResponse),
        no_throttle_probe!(sasl_authenticate_response, SaslAuthenticateResponse),
        probe!(create_partitions_response, CreatePartitionsResponse),
        probe!(
            create_delegation_token_response,
            CreateDelegationTokenResponse
        ),
        probe!(
            renew_delegation_token_response,
            RenewDelegationTokenResponse
        ),
        probe!(
            expire_delegation_token_response,
            ExpireDelegationTokenResponse
        ),
        probe!(
            describe_delegation_token_response,
            DescribeDelegationTokenResponse
        ),
        probe!(delete_groups_response, DeleteGroupsResponse),
        probe!(elect_leaders_response, ElectLeadersResponse),
        probe!(
            incremental_alter_configs_response,
            IncrementalAlterConfigsResponse
        ),
        probe!(
            alter_partition_reassignments_response,
            AlterPartitionReassignmentsResponse
        ),
        probe!(
            list_partition_reassignments_response,
            ListPartitionReassignmentsResponse
        ),
        probe!(offset_delete_response, OffsetDeleteResponse),
        probe!(
            describe_client_quotas_response,
            DescribeClientQuotasResponse
        ),
        probe!(alter_client_quotas_response, AlterClientQuotasResponse),
        probe!(
            describe_user_scram_credentials_response,
            DescribeUserScramCredentialsResponse
        ),
        probe!(
            alter_user_scram_credentials_response,
            AlterUserScramCredentialsResponse
        ),
        no_throttle_probe!(describe_quorum_response, DescribeQuorumResponse),
        probe!(alter_partition_response, AlterPartitionResponse),
        probe!(update_features_response, UpdateFeaturesResponse),
        probe!(fetch_snapshot_response, FetchSnapshotResponse),
        probe!(describe_cluster_response, DescribeClusterResponse),
        probe!(describe_producers_response, DescribeProducersResponse),
        probe!(broker_registration_response, BrokerRegistrationResponse),
        probe!(broker_heartbeat_response, BrokerHeartbeatResponse),
        probe!(unregister_broker_response, UnregisterBrokerResponse),
        probe!(describe_transactions_response, DescribeTransactionsResponse),
        probe!(list_transactions_response, ListTransactionsResponse),
        probe!(allocate_producer_ids_response, AllocateProducerIdsResponse),
        probe!(
            consumer_group_heartbeat_response,
            ConsumerGroupHeartbeatResponse
        ),
        probe!(
            consumer_group_describe_response,
            ConsumerGroupDescribeResponse
        ),
        probe!(
            controller_registration_response,
            ControllerRegistrationResponse
        ),
        probe!(
            get_telemetry_subscriptions_response,
            GetTelemetrySubscriptionsResponse
        ),
        probe!(push_telemetry_response, PushTelemetryResponse),
        probe!(
            assign_replicas_to_dirs_response,
            AssignReplicasToDirsResponse
        ),
        probe!(list_config_resources_response, ListConfigResourcesResponse),
        probe!(
            describe_topic_partitions_response,
            DescribeTopicPartitionsResponse
        ),
        probe!(share_group_heartbeat_response, ShareGroupHeartbeatResponse),
        probe!(share_group_describe_response, ShareGroupDescribeResponse),
        probe!(share_fetch_response, ShareFetchResponse),
        probe!(share_acknowledge_response, ShareAcknowledgeResponse),
        probe!(add_raft_voter_response, AddRaftVoterResponse),
        probe!(remove_raft_voter_response, RemoveRaftVoterResponse),
        probe!(update_raft_voter_response, UpdateRaftVoterResponse),
        no_throttle_probe!(
            initialize_share_group_state_response,
            InitializeShareGroupStateResponse
        ),
        no_throttle_probe!(read_share_group_state_response, ReadShareGroupStateResponse),
        no_throttle_probe!(
            write_share_group_state_response,
            WriteShareGroupStateResponse
        ),
        no_throttle_probe!(
            delete_share_group_state_response,
            DeleteShareGroupStateResponse
        ),
        no_throttle_probe!(
            read_share_group_state_summary_response,
            ReadShareGroupStateSummaryResponse
        ),
        probe!(
            streams_group_heartbeat_response,
            StreamsGroupHeartbeatResponse
        ),
        probe!(
            streams_group_describe_response,
            StreamsGroupDescribeResponse
        ),
        probe!(
            describe_share_group_offsets_response,
            DescribeShareGroupOffsetsResponse
        ),
        probe!(
            alter_share_group_offsets_response,
            AlterShareGroupOffsetsResponse
        ),
        probe!(
            delete_share_group_offsets_response,
            DeleteShareGroupOffsetsResponse
        ),
        no_throttle_probe!(get_replica_log_info_response, GetReplicaLogInfoResponse),
    ]
    .into_iter()
    .collect()
}

/// Every `(api_key, version)` pair the broker advertises, in ascending order.
fn advertised_pairs() -> Vec<(ApiKeyCode, ApiVersion)> {
    let mut pairs: Vec<(ApiKeyCode, ApiVersion)> = supported_apis()
        .iter()
        .flat_map(|api| {
            let key = api.api_key;
            (api.min_version..=api.max_version).map(move |version| (key, version))
        })
        .collect();
    pairs.sort_unstable();
    pairs
}

/// A `(api_key, version)` pair where the table and the encoder disagree.
#[derive(Debug, PartialEq, Eq)]
struct Mismatch {
    api_key: ApiKeyCode,
    version: ApiVersion,
    encoder: ThrottlePosition,
    table_says_leading: bool,
}

#[test]
fn every_advertised_api_is_classified() {
    let probes = probes();
    let mut unclassified: Vec<ApiKeyCode> = advertised_pairs()
        .into_iter()
        .map(|(api_key, _)| api_key)
        .filter(|api_key| !probes.contains_key(api_key))
        .collect();
    unclassified.dedup();

    assert!(
        unclassified == Vec::<ApiKeyCode>::new(),
        "advertised api_keys with no throttle-echo probe: add them to \
         `throttle_audit::probes` and classify them in \
         `response::throttle_is_leading_field`"
    );
}

#[test]
fn throttle_table_matches_every_advertised_response_schema() {
    let probes = probes();
    let mismatches: Vec<Mismatch> = advertised_pairs()
        .into_iter()
        .filter_map(|(api_key, version)| {
            let encoder = probes.get(&api_key)?(version);
            let table_says_leading = throttle_is_leading_field(api_key, version);
            (table_says_leading != (encoder == ThrottlePosition::Leading)).then_some(Mismatch {
                api_key,
                version,
                encoder,
                table_says_leading,
            })
        })
        .collect();

    assert!(mismatches == Vec::<Mismatch>::new());
}

#[test]
fn throttle_echo_divergences_are_the_recorded_ones() {
    let probes = probes();
    let buried: Vec<(ApiKeyCode, ApiVersion)> = advertised_pairs()
        .into_iter()
        .filter(|&(api_key, version)| {
            probes
                .get(&api_key)
                .is_some_and(|probe| probe(version) == ThrottlePosition::Buried)
        })
        .collect();

    let mut recorded: Vec<(ApiKeyCode, ApiVersion)> = THROTTLE_ECHO_DIVERGENCES
        .iter()
        .flat_map(|&(api_key, min, max)| (min..=max).map(move |version| (api_key, version)))
        .collect();
    recorded.sort_unstable();

    assert!(buried == recorded);
}

#[test]
fn sentinel_probe_detects_a_leading_throttle_and_a_buried_one() {
    use krabka_protocol::owned::{
        metadata_response::{self, MetadataResponse},
        offset_delete_response::{self, OffsetDeleteResponse},
    };

    // Metadata moved ThrottleTimeMs to the front at v3, so the same struct
    // reads Absent at v2 and Leading at v3.
    let metadata = MetadataResponse {
        throttle_time_ms: SENTINEL,
        ..Default::default()
    };
    assert!(
        position(&metadata, 2, &metadata_response::default_json(2)) == ThrottlePosition::Absent
    );
    assert!(
        position(&metadata, 3, &metadata_response::default_json(3)) == ThrottlePosition::Leading
    );

    // OffsetDelete keeps ErrorCode in front of it at every version.
    let offset_delete = OffsetDeleteResponse {
        throttle_time_ms: SENTINEL,
        ..Default::default()
    };
    assert!(
        position(&offset_delete, 0, &offset_delete_response::default_json(0))
            == ThrottlePosition::Buried
    );
}
