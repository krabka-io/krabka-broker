//! The API version table of the Kafka release krabka matches by default.
//!
//! `krabka-protocol` vendors Kafka trunk's message schemas, which run ahead of
//! the latest Kafka release. [`KAFKA_4_3_1_APIS`] is what a stock
//! `apache/kafka:4.3.1` accepts instead: every request schema's
//! `validVersions` in `clients/src/main/resources/common/message` at the
//! `4.3.1` tag, with the one version 4.3.1 marks `latestVersionUnstable`
//! (`InitProducerId` v6) left out, since that release advertises it only under
//! `unstable.api.versions.enable` too. It is what both listeners serve while
//! [`UnstableApiVersions::Disabled`](crate::UnstableApiVersions) holds.

/// One Kafka 4.3.1 request schema: its api key and the versions the release
/// accepts by default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleasedApi {
    pub api_key: i16,
    /// `validVersions`' lower bound. `Produce` advertises from 0 regardless
    /// (`ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`, KAFKA-18659).
    pub min_version: i16,
    /// `validVersions`' upper bound, below a `latestVersionUnstable` version.
    pub max_version: i16,
}

const fn api(api_key: i16, min_version: i16, max_version: i16) -> ReleasedApi {
    ReleasedApi {
        api_key,
        min_version,
        max_version,
    }
}

/// Every api key Kafka 4.3.1 has a valid version of, in key order.
pub const KAFKA_4_3_1_APIS: &[ReleasedApi] = &[
    api(0, 3, 13), // Produce
    api(1, 4, 18), // Fetch
    api(2, 1, 11), // ListOffsets
    api(3, 0, 13), // Metadata
    api(8, 2, 10), // OffsetCommit
    api(9, 1, 10), // OffsetFetch
    api(10, 0, 6), // FindCoordinator
    api(11, 0, 9), // JoinGroup
    api(12, 0, 4), // Heartbeat
    api(13, 0, 5), // LeaveGroup
    api(14, 0, 5), // SyncGroup
    api(15, 0, 6), // DescribeGroups
    api(16, 0, 5), // ListGroups
    api(17, 0, 1), // SaslHandshake
    api(18, 0, 4), // ApiVersions
    api(19, 2, 7), // CreateTopics
    api(20, 1, 6), // DeleteTopics
    api(21, 0, 2), // DeleteRecords
    api(22, 0, 5), // InitProducerId
    api(23, 2, 4), // OffsetForLeaderEpoch
    api(24, 0, 5), // AddPartitionsToTxn
    api(25, 0, 4), // AddOffsetsToTxn
    api(26, 0, 5), // EndTxn
    api(27, 1, 2), // WriteTxnMarkers
    api(28, 0, 5), // TxnOffsetCommit
    api(29, 1, 3), // DescribeAcls
    api(30, 1, 3), // CreateAcls
    api(31, 1, 3), // DeleteAcls
    api(32, 1, 4), // DescribeConfigs
    api(33, 0, 2), // AlterConfigs
    api(34, 1, 2), // AlterReplicaLogDirs
    api(35, 1, 5), // DescribeLogDirs
    api(36, 0, 2), // SaslAuthenticate
    api(37, 0, 3), // CreatePartitions
    api(38, 1, 3), // CreateDelegationToken
    api(39, 1, 2), // RenewDelegationToken
    api(40, 1, 2), // ExpireDelegationToken
    api(41, 1, 3), // DescribeDelegationToken
    api(42, 0, 2), // DeleteGroups
    api(43, 0, 2), // ElectLeaders
    api(44, 0, 1), // IncrementalAlterConfigs
    api(45, 0, 1), // AlterPartitionReassignments
    api(46, 0, 0), // ListPartitionReassignments
    api(47, 0, 0), // OffsetDelete
    api(48, 0, 1), // DescribeClientQuotas
    api(49, 0, 1), // AlterClientQuotas
    api(50, 0, 0), // DescribeUserScramCredentials
    api(51, 0, 0), // AlterUserScramCredentials
    api(52, 0, 2), // Vote
    api(53, 0, 1), // BeginQuorumEpoch
    api(54, 0, 1), // EndQuorumEpoch
    api(55, 0, 2), // DescribeQuorum
    api(56, 2, 3), // AlterPartition
    api(57, 0, 2), // UpdateFeatures
    api(58, 0, 0), // Envelope
    api(59, 0, 1), // FetchSnapshot
    api(60, 0, 2), // DescribeCluster
    api(61, 0, 0), // DescribeProducers
    api(62, 0, 4), // BrokerRegistration
    api(63, 0, 2), // BrokerHeartbeat
    api(64, 0, 0), // UnregisterBroker
    api(65, 0, 0), // DescribeTransactions
    api(66, 0, 2), // ListTransactions
    api(67, 0, 0), // AllocateProducerIds
    api(68, 0, 1), // ConsumerGroupHeartbeat
    api(69, 0, 1), // ConsumerGroupDescribe
    api(70, 0, 0), // ControllerRegistration
    api(71, 0, 0), // GetTelemetrySubscriptions
    api(72, 0, 0), // PushTelemetry
    api(73, 0, 0), // AssignReplicasToDirs
    api(74, 0, 1), // ListConfigResources
    api(75, 0, 0), // DescribeTopicPartitions
    api(76, 1, 1), // ShareGroupHeartbeat
    api(77, 1, 1), // ShareGroupDescribe
    api(78, 1, 2), // ShareFetch
    api(79, 1, 2), // ShareAcknowledge
    api(80, 0, 1), // AddRaftVoter
    api(81, 0, 0), // RemoveRaftVoter
    api(82, 0, 0), // UpdateRaftVoter
    api(83, 0, 0), // InitializeShareGroupState
    api(84, 0, 0), // ReadShareGroupState
    api(85, 0, 1), // WriteShareGroupState
    api(86, 0, 0), // DeleteShareGroupState
    api(87, 0, 1), // ReadShareGroupStateSummary
    api(88, 0, 0), // StreamsGroupHeartbeat
    api(89, 0, 0), // StreamsGroupDescribe
    api(90, 0, 1), // DescribeShareGroupOffsets
    api(91, 0, 0), // AlterShareGroupOffsets
    api(92, 0, 0), // DeleteShareGroupOffsets
];

/// The Kafka 4.3.1 row for `api_key`, or `None` when that release has no such
/// api key.
#[must_use]
pub const fn kafka_4_3_1_api(api_key: i16) -> Option<ReleasedApi> {
    let mut rest: &[ReleasedApi] = KAFKA_4_3_1_APIS;
    while let [row, tail @ ..] = rest {
        if row.api_key == api_key {
            return Some(*row);
        }
        rest = tail;
    }
    None
}

/// The highest version of `api_key` Kafka 4.3.1 accepts by default, or `None`
/// when that release has no such api key.
#[must_use]
pub const fn kafka_4_3_1_max(api_key: i16) -> Option<i16> {
    match kafka_4_3_1_api(api_key) {
        Some(api) => Some(api.max_version),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    struct ReleaseCatalogue {
        keys: Vec<i16>,
        key: Option<i16>,
        rows: Option<Vec<ReleasedApi>>,
        maximum: Slot<Option<i16>>,
        row: Slot<Option<ReleasedApi>>,
    }

    #[fixture]
    fn release() -> ReleaseCatalogue {
        // Start this scenario with fresh inputs and observations.

        ReleaseCatalogue {
            keys: Vec::new(),
            key: None,
            rows: None,
            maximum: Slot::default(),
            row: Slot::default(),
        }
    }

    #[given("every API key published in the Kafka 4.3.1 catalogue")]
    fn published_apis(release: &mut ReleaseCatalogue) {
        release.keys = KAFKA_4_3_1_APIS.iter().map(|row| row.api_key).collect();
    }

    #[when("every published API is resolved through the release lookup")]
    fn resolve_catalogue(release: &mut ReleaseCatalogue) {
        release.rows = Some(
            release
                .keys
                .iter()
                .map(|&key| kafka_4_3_1_api(key).expect("every published key must resolve"))
                .collect(),
        );
    }

    #[then("the release exposes 89 unique ascending API keys with nonempty version ranges")]
    fn valid_catalogue(release: &ReleaseCatalogue) {
        let rows = release.rows.as_ref().expect("the catalogue was resolved");
        check!(
            rows.windows(2)
                .all(|pair| pair[0].api_key < pair[1].api_key)
        );
        check!(rows.len() == 89);
        for row in rows {
            check!(row.min_version <= row.max_version, "{row:?}");
        }
    }

    #[given("an API key {key} requested from the Kafka 4.3.1 catalogue")]
    fn requested_api(release: &mut ReleaseCatalogue, key: i16) { release.key = Some(key); }

    #[when("the released maximum API version is looked up")]
    fn maximum_version(release: &mut ReleaseCatalogue) {
        release
            .maximum
            .set(kafka_4_3_1_max(release.key.expect("an API key was given")));
    }

    #[then("the released maximum API version is {version}")]
    fn released_maximum(release: &ReleaseCatalogue, version: String) {
        let expected = if version == "unavailable" {
            None
        } else {
            Some(version.parse().expect("an API version is an integer"))
        };
        check!(release.maximum.get() == Some(expected));
    }

    #[when("the released API version range is looked up")]
    fn api_range(release: &mut ReleaseCatalogue) {
        release
            .row
            .set(kafka_4_3_1_api(release.key.expect("an API key was given")));
    }

    #[then("the released Fetch API spans versions 4 through 18")]
    fn fetch_versions(release: &ReleaseCatalogue) {
        check!(release.row.get() == Some(Some(api(1, 4, 18))));
    }

    scenarios!("tests/features/migrated/src/config/kafka_release.feature", fixtures = [release: ReleaseCatalogue]);
}
