//! Tests for the assembled dispatch table: which `api_key` maps to which
//! handler kind, and how the table agrees with the advertised api catalog.

use {
    super::*,
    crate::{
        api_catalog::{INTER_BROKER_ONLY_APIS, dispatched_apis},
        handlers::{self, ApiKeyCode, KRABKA_PRIVATE_API_KEY_FLOOR},
    },
    assert2::assert,
    std::{
        collections::{BTreeMap, BTreeSet},
        ptr,
    },
};

#[test]
fn registry_registers_raw_context_handlers() {
    let registry = build_registry();

    for api_key in [
        ApiKey::Produce as i16,
        ApiKey::AssignReplicasToDirs as i16,
        ApiKey::Metadata as i16,
        ApiKey::OffsetCommit as i16,
        ApiKey::OffsetFetch as i16,
        ApiKey::FindCoordinator as i16,
        ApiKey::JoinGroup as i16,
        ApiKey::Heartbeat as i16,
        ApiKey::LeaveGroup as i16,
        ApiKey::SyncGroup as i16,
        ApiKey::DeleteGroups as i16,
        ApiKey::ListOffsets as i16,
        ApiKey::OffsetForLeaderEpoch as i16,
        ApiKey::CreateTopics as i16,
        ApiKey::DeleteTopics as i16,
        ApiKey::AlterConfigs as i16,
        ApiKey::IncrementalAlterConfigs as i16,
        ApiKey::DeleteRecords as i16,
        ApiKey::CreatePartitions as i16,
        ApiKey::DescribeGroups as i16,
        ApiKey::ListGroups as i16,
        ApiKey::OffsetDelete as i16,
        ApiKey::DescribeCluster as i16,
        ApiKey::DescribeProducers as i16,
        ApiKey::DescribeTransactions as i16,
        ApiKey::ListTransactions as i16,
        ApiKey::UnregisterBroker as i16,
        ApiKey::UnregisterController as i16,
        ApiKey::StreamsGroupTopologyDescriptionUpdate as i16,
        ApiKey::DescribeTopicPartitions as i16,
        ApiKey::ListConfigResources as i16,
        ApiKey::DescribeQuorum as i16,
        ApiKey::AddRaftVoter as i16,
        ApiKey::RemoveRaftVoter as i16,
        ApiKey::UpdateRaftVoter as i16,
        ApiKey::AlterPartition as i16,
        ApiKey::BrokerHeartbeat as i16,
        ApiKey::GetReplicaLogInfo as i16,
        ApiKey::ConsumerGroupHeartbeat as i16,
        ApiKey::ConsumerGroupDescribe as i16,
        ApiKey::ShareGroupDescribe as i16,
        ApiKey::ShareFetch as i16,
        ApiKey::ShareAcknowledge as i16,
        ApiKey::ShareGroupHeartbeat as i16,
        ApiKey::StreamsGroupHeartbeat as i16,
        ApiKey::StreamsGroupDescribe as i16,
        ApiKey::DescribeShareGroupOffsets as i16,
        ApiKey::AlterShareGroupOffsets as i16,
        ApiKey::DeleteShareGroupOffsets as i16,
        ApiKey::InitProducerId as i16,
        ApiKey::AddPartitionsToTxn as i16,
        ApiKey::EndTxn as i16,
        ApiKey::TxnOffsetCommit as i16,
        ApiKey::AddOffsetsToTxn as i16,
        ApiKey::WriteTxnMarkers as i16,
        ApiKey::FetchSnapshot as i16,
        ApiKey::AllocateProducerIds as i16,
        ApiKey::InitializeShareGroupState as i16,
        ApiKey::ReadShareGroupState as i16,
        ApiKey::WriteShareGroupState as i16,
        ApiKey::DeleteShareGroupState as i16,
        ApiKey::ReadShareGroupStateSummary as i16,
    ] {
        let key = api_key;
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(
                entry.kind(),
                DispatchKind::Context(_) | DispatchKind::Produce(_)
            ),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_telemetry_handlers() {
    let registry = build_registry();

    for key in [71, 72] {
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Telemetry(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_decoded_context_handlers() {
    let registry = build_registry();

    for api_key in [
        ApiKey::DescribeAcls as i16,
        ApiKey::CreateAcls as i16,
        ApiKey::DeleteAcls as i16,
        ApiKey::ElectLeaders as i16,
        ApiKey::AlterPartitionReassignments as i16,
        ApiKey::ListPartitionReassignments as i16,
        ApiKey::DescribeClientQuotas as i16,
        ApiKey::AlterClientQuotas as i16,
        ApiKey::DescribeUserScramCredentials as i16,
        ApiKey::AlterUserScramCredentials as i16,
        ApiKey::UpdateFeatures as i16,
    ] {
        let target_key = api_key;
        let entry = registry
            .get(target_key)
            .unwrap_or_else(|| panic!("registered api_key {target_key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Context(_)),
            "api_key {target_key}"
        );
    }
}

#[test]
fn registry_registers_auth_handlers() {
    let registry = build_registry();

    for key in [34, 38, 39, 40, 41] {
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Auth(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_reports_missing_keys() {
    let registry = build_registry();

    assert!(registry.get(9999).is_none());
}

struct RegistryContract {
    registry: Option<DispatchRegistry>,
    registered: Option<BTreeSet<ApiKeyCode>>,
    advertised: Option<BTreeSet<ApiKeyCode>>,
    policies: Option<BTreeMap<ApiKeyCode, RequestQuotaPolicy>>,
}

#[rstest::fixture]
fn registry_contract() -> RegistryContract {
    // Start this scenario with fresh inputs and observations.

    RegistryContract {
        registry: None,
        registered: None,
        advertised: None,
        policies: None,
    }
}

#[rstest_bdd_macros::given("the broker's complete handler dispatch registry is assembled")]
fn assembled_dispatch_registry(registry_contract: &mut RegistryContract) {
    registry_contract.registry = Some(build_registry());
}

#[rstest_bdd_macros::when("the registered and advertised Kafka API keys are enumerated")]
fn enumerate_dispatch_apis(registry_contract: &mut RegistryContract) {
    let registry = registry_contract
        .registry
        .as_ref()
        .expect("the registry was assembled");
    registry_contract.registered = Some(registry.registered_api_keys().collect());
    registry_contract.advertised = Some(
        dispatched_apis()
            .into_iter()
            .map(|api| api.api_key)
            .collect(),
    );
}

#[rstest_bdd_macros::when("the dispatch request-quota policy for every registered API is read")]
fn read_dispatch_quota_policies(registry_contract: &mut RegistryContract) {
    let registry = registry_contract
        .registry
        .as_ref()
        .expect("the registry was assembled");
    registry_contract.policies = Some(
        registry
            .registered_api_keys()
            .map(|key| {
                (
                    key,
                    registry.get(key).expect("a registered API").quota_policy(),
                )
            })
            .collect(),
    );
}

#[rstest_bdd_macros::then(
    "every advertised API is registered and every registered schema API is advertised"
)]
fn advertised_registry_coverage(registry_contract: &RegistryContract) {
    let registered = registry_contract
        .registered
        .as_ref()
        .expect("registered keys were read");
    let advertised = registry_contract
        .advertised
        .as_ref()
        .expect("advertised keys were read");
    let with_schema: BTreeSet<_> = registered
        .iter()
        .copied()
        .filter(|key| ApiKey::from_i16(*key).is_some())
        .collect();
    assert!(!registered.is_empty());
    assert!(advertised.is_subset(registered));
    assert!(with_schema == *advertised);
}

#[rstest_bdd_macros::then("every registered API retains Kafka's request-quota accounting policy")]
fn dispatch_quota_accounting(registry_contract: &RegistryContract) {
    let observed = registry_contract
        .policies
        .as_ref()
        .expect("all quota policies were read");
    assert!(!observed.is_empty());
    let self_accounted = [
        ApiKey::Produce,
        ApiKey::Fetch,
        ApiKey::ShareFetch,
        ApiKey::ApiVersions,
    ];
    let exempt = [
        ApiKey::SaslHandshake,
        ApiKey::SaslAuthenticate,
        ApiKey::WriteTxnMarkers,
        ApiKey::AlterPartition,
        ApiKey::FetchSnapshot,
        ApiKey::UpdateRaftVoter,
    ];
    let expected: BTreeMap<_, _> = observed
        .keys()
        .map(|&key| {
            let policy = if self_accounted.iter().any(|api| *api as i16 == key) {
                RequestQuotaPolicy::SelfAccounted
            } else if exempt.iter().any(|api| *api as i16 == key)
                || (key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR
                    && ApiKey::from_i16(key).is_none())
            {
                RequestQuotaPolicy::InlineExempt
            } else {
                RequestQuotaPolicy::ApplyFallbackAccounting
            };
            (key, policy)
        })
        .collect();
    assert!(*observed == expected);
}

rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/registry/tests.feature", fixtures = [registry_contract: RegistryContract]);

/// The only krabka-private key in the catalog is `GetReplicaLogInfo` (1020),
/// and it is inter-broker only, so no listener a client can reach advertises
/// it and `kafka-broker-api-versions` never prints an `UNKNOWN(1020)` row.
#[test]
fn the_only_catalogued_krabka_private_key_is_inter_broker_only() {
    let floor = KRABKA_PRIVATE_API_KEY_FLOOR;
    let private: Vec<ApiKeyCode> = dispatched_apis()
        .into_iter()
        .map(|api| api.api_key)
        .filter(|key| *key >= floor)
        .collect();

    assert!(private == vec![ApiKey::GetReplicaLogInfo as i16]);
    assert!(ApiKey::GetReplicaLogInfo as i16 == 1020);
    assert!(INTER_BROKER_ONLY_APIS.contains(&1020));
}

#[test]
fn registry_version_bounds_match_api_catalog() {
    let registry = build_registry();

    for api in dispatched_apis() {
        let entry = registry
            .get(api.api_key)
            .unwrap_or_else(|| panic!("registered api_key {}", api.api_key));
        assert!(
            entry.version_range() == (api.min_version..=api.max_version),
            "api_key {}",
            api.api_key
        );
    }
}

#[test]
fn registry_body_flexible_matches_selected_schema_boundaries() {
    use krabka_protocol::owned;

    let registry = build_registry();
    let cases = [
        (0, owned::produce_request::FLEXIBLE_MIN - 1, false),
        (0, owned::produce_request::FLEXIBLE_MIN, true),
        (1, owned::fetch_request::FLEXIBLE_MIN - 1, false),
        (1, owned::fetch_request::FLEXIBLE_MIN, true),
        (
            36,
            owned::sasl_authenticate_request::FLEXIBLE_MIN - 1,
            false,
        ),
        (36, owned::sasl_authenticate_request::FLEXIBLE_MIN, true),
        (17, i16::MAX, false),
        (999, 0, false),
    ];

    for (api_key, version, want) in cases {
        assert!(
            registry.body_flexible(api_key, version) == want,
            "api_key {api_key} version {version}"
        );
    }
}

/// `ApiVersions` is the one context dispatch that charges the request quota
/// itself: its `ThrottleTimeMs` sits behind the `ApiKeys` array, so the
/// dispatch loop's leading-int32 patch cannot report a delay on it and the
/// handler fills the field in instead.
#[test]
fn api_versions_is_a_self_accounted_context_dispatch() {
    let registry = build_registry();
    let entry = registry
        .get(ApiKey::ApiVersions as i16)
        .expect("ApiVersions");

    assert!(entry.is_context());
    assert!(entry.quota_policy() == RequestQuotaPolicy::SelfAccounted);
    assert!(entry.body_flexible(3));
    assert!(!entry.body_flexible(2));

    let handler = registry
        .get_context(ApiKey::ApiVersions as i16)
        .expect("context ApiVersions handler");
    assert!(ptr::fn_addr_eq(
        handler,
        handlers::api_versions::handle as ContextHandler
    ));
}
