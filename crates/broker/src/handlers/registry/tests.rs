//! Tests for the assembled dispatch table: which `api_key` maps to which
//! handler kind, and how the table agrees with the advertised api catalog.

use std::collections::BTreeSet;

use assert2::assert;

use super::*;
use crate::handlers::{self, ApiKeyCode};

#[test]
fn registry_registers_plain_handlers() {
    let registry = build_registry();

    let api_versions = registry
        .get(ApiKey::ApiVersions as i16)
        .expect("ApiVersions");
    assert!(api_versions.is_plain());
    assert!(api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting);
    assert!(api_versions.body_flexible(3));
    assert!(!api_versions.body_flexible(2));

    for key in [25, 27, 59, 73, 83, 84, 85, 86, 87] {
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(entry.is_plain(), "api_key {key}");
    }
}

#[test]
fn registry_registers_raw_context_handlers() {
    let registry = build_registry();

    for api_key in [
        ApiKey::Produce as i16,
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
        let key = api_key;
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Context(_)),
            "api_key {key}"
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

#[test]
fn registry_and_api_catalog_cover_the_same_kafka_api_keys() {
    let registry = build_registry();
    let registered: BTreeSet<ApiKeyCode> = registry.registered_api_keys().collect();
    let advertised: BTreeSet<ApiKeyCode> = crate::api_catalog::supported_apis()
        .into_iter()
        .map(|api| api.api_key)
        .collect();

    let floor = crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR;
    let registered_kafka: BTreeSet<ApiKeyCode> = registered
        .iter()
        .copied()
        .filter(|key| *key < floor)
        .collect();

    // Every advertised key is registered, and every registered Kafka key is
    // advertised. The krabka-private keys are deliberately absent from the
    // catalog: advertising them would put UNKNOWN(1010) rows into
    // kafka-broker-api-versions output, a visible divergence from a real
    // broker, and a client that does not find a key negotiates (0, 0),
    // which is right for a MIN = MAX = 0 request.
    assert!(advertised.is_subset(&registered));
    assert!(registered_kafka == advertised);
    assert!(advertised.iter().all(|key| *key < floor));
}

#[test]
fn registry_version_bounds_match_api_catalog() {
    let registry = build_registry();

    for api in crate::api_catalog::supported_apis() {
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

#[test]
fn plain_handler_pointer_matches_existing_api_versions_handler() {
    let registry = build_registry();
    let handler = registry
        .get_plain(ApiKey::ApiVersions as i16)
        .expect("plain ApiVersions handler");

    assert!(std::ptr::fn_addr_eq(
        handler,
        handlers::api_versions::handle as PlainHandler
    ));
}
