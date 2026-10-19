//! Committed consumer-group offsets: the one restore input that is not a file
//! on a node.
//!
//! `__consumer_offsets` is compacted and internal, so it is never tiered and a
//! KIP-405 archive does not hold it. A restore therefore rebuilds the log and
//! nothing that points into it, and every group starts again from
//! `auto.offset.reset`. What can be copied is the coordinator's answer: the
//! committed offset of every group, read with `ListGroups` and `OffsetFetch`
//! while the cluster is up, and written back with `OffsetCommit` once a
//! restored cluster is.
//!
//! The write-back is Kafka's `Admin.alterConsumerGroupOffsets`, the call
//! `kafka-consumer-groups --reset-offsets --execute` makes: a simple-consumer
//! commit, with an empty `member_id` and generation `-1`, that the coordinator
//! accepts for a group with no live members. A group with live members is
//! fenced, which is the correct answer: an offset must not move under a
//! consumer that is reading from it. The admin client retries the coordinator
//! codes Kafka's `AlterConsumerGroupOffsetsHandler` retries, so a freshly
//! booted broker that is still creating `__consumer_offsets` is waited for
//! rather than reported as a refusal.

use {
    crate::{error::BackupError, manifest::decode_versioned},
    krabka_client_admin::{
        AdminError,
        groups::{ConsumerGroupOffsetOutcome, GroupOffsets as AdminGroupOffsets},
    },
    krabka_client_core::ClientError,
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// One group's committed offset in one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedOffset {
    /// Topic the offset was committed for.
    pub topic: String,
    /// Partition index within that topic.
    pub partition: i32,
    /// The committed offset: the next offset the group would read.
    pub offset: i64,
}

/// One group, with every partition it has a committed offset for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupOffsets {
    /// The group id.
    pub group: String,
    /// Its committed offsets, in `(topic, partition)` order.
    pub offsets: Vec<CommittedOffset>,
}

/// Version of `group-offsets.json`, its required top-level `"version"` field.
///
/// Part of the 1.x on-disk contract: a 1.x `krabka-backup` reads every offsets
/// file that any earlier 1.x build wrote.
pub const GROUP_OFFSETS_VERSION: i16 = 0;

/// The captured offsets of every group in one cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupOffsetsFile {
    /// Always [`GROUP_OFFSETS_VERSION`] when this build writes it.
    pub version: i16,
    /// One entry per group that had at least one committed offset.
    pub groups: Vec<GroupOffsets>,
}

/// An empty file of the version this build writes.
impl Default for GroupOffsetsFile {
    fn default() -> Self {
        Self {
            version: GROUP_OFFSETS_VERSION,
            groups: Vec::new(),
        }
    }
}

impl GroupOffsetsFile {
    /// Decode an offsets file, refusing a missing or unknown `"version"`.
    ///
    /// # Errors
    ///
    /// [`BackupError::UnsupportedVersion`] for a file with no version or one
    /// other than [`GROUP_OFFSETS_VERSION`], and [`BackupError::Json`] for
    /// bytes that are not an offsets file. `context` names the object in
    /// either error.
    pub fn from_slice(bytes: &[u8], context: &str) -> Result<Self, BackupError> {
        decode_versioned(bytes, context, GROUP_OFFSETS_VERSION)
    }

    /// How many `(group, topic, partition)` offsets the file holds.
    #[must_use]
    pub fn offset_count(&self) -> usize {
        self.groups.iter().map(|group| group.offsets.len()).sum()
    }
}

/// Turn one `OffsetFetch` answer into the captured shape.
///
/// The answer arrives as a map, so the ordering is already `(topic, partition)`
/// and the captured file is byte-stable across two captures of an unchanged
/// cluster. That is what lets an operator diff two captures.
#[must_use]
pub fn group_offsets(group: &str, fetched: &BTreeMap<(String, i32), i64>) -> GroupOffsets {
    GroupOffsets {
        group: group.to_owned(),
        offsets: fetched
            .iter()
            .map(|((topic, partition), offset)| CommittedOffset {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            })
            .collect(),
    }
}

/// One group's captured offsets, keyed the way the admin client commits them.
#[must_use]
pub fn admin_offsets(offsets: &GroupOffsets) -> AdminGroupOffsets {
    offsets
        .offsets
        .iter()
        .map(|offset| ((offset.topic.clone(), offset.partition), offset.offset))
        .collect()
}

/// Every partition the coordinator refused, as `topic-partition: NAME (code)`.
///
/// A commit is per-partition on the wire, so a partial refusal is possible and
/// silence about it would report a restore that put back fewer offsets than it
/// claims.
#[must_use]
pub fn commit_refusals(outcomes: &[ConsumerGroupOffsetOutcome]) -> Vec<String> {
    outcomes
        .iter()
        .filter_map(|outcome| {
            outcome.error.as_ref().map(|error| {
                format!(
                    "{}-{}: {} ({})",
                    outcome.topic, outcome.partition, error.name, error.code
                )
            })
        })
        .collect()
}

/// How many offsets one group's commit put back, or why it put back none.
///
/// The admin client retries `COORDINATOR_LOAD_IN_PROGRESS`,
/// `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR` and `REBALANCE_IN_PROGRESS`
/// until Kafka's `default.api.timeout.ms` runs out, so a timeout means the
/// group's coordinator never became ready, not that the cluster refused.
///
/// # Errors
///
/// [`BackupError::Cluster`] when the call failed or timed out, or when the
/// coordinator refused any partition.
pub fn commit_outcome(
    offsets: &GroupOffsets,
    result: Result<Vec<ConsumerGroupOffsetOutcome>, AdminError>,
) -> Result<usize, BackupError> {
    let group = &offsets.group;
    let outcomes = result.map_err(|error| match error {
        AdminError::Transport(ClientError::Timeout(_)) => BackupError::Cluster(format!(
            "OffsetCommit for {group}: {error}: the group coordinator did not become \
             available. A broker creates and loads `__consumer_offsets` on the first \
             coordinator lookup; check the broker log, for example for \
             INVALID_REPLICATION_FACTOR"
        )),
        error => BackupError::Cluster(format!("OffsetCommit for {group}: {error}")),
    })?;
    let refusals = commit_refusals(&outcomes);
    if !refusals.is_empty() {
        return Err(BackupError::Cluster(format!(
            "OffsetCommit for {group}: {}",
            refusals.join("; ")
        )));
    }
    Ok(offsets.offsets.len())
}

#[cfg(test)]
mod tests {
    use {
        super::{
            CommittedOffset, GROUP_OFFSETS_VERSION, GroupOffsets, GroupOffsetsFile, admin_offsets,
            commit_outcome, commit_refusals, group_offsets,
        },
        crate::error::BackupError,
        assert2::check,
        krabka_client_admin::{AdminError, KafkaError, groups::ConsumerGroupOffsetOutcome},
        krabka_client_core::ClientError,
        std::collections::BTreeMap,
    };

    fn captured() -> GroupOffsets {
        GroupOffsets {
            group: "orders-consumers".to_owned(),
            offsets: vec![
                CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 0,
                    offset: 12,
                },
                CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 1,
                    offset: 7,
                },
                CommittedOffset {
                    topic: "payments".to_owned(),
                    partition: 0,
                    offset: 3,
                },
            ],
        }
    }

    #[test]
    fn a_fetched_map_becomes_offsets_in_topic_and_partition_order() {
        let fetched = BTreeMap::from([
            (("payments".to_owned(), 0), 3),
            (("orders".to_owned(), 1), 7),
            (("orders".to_owned(), 0), 12),
        ]);
        check!(group_offsets("orders-consumers", &fetched) == captured());
    }

    #[test]
    fn the_commit_carries_every_captured_partition() {
        check!(
            admin_offsets(&captured())
                == BTreeMap::from([
                    (("orders".to_owned(), 0), 12),
                    (("orders".to_owned(), 1), 7),
                    (("payments".to_owned(), 0), 3),
                ])
        );
    }

    fn outcome(partition: i32, error: Option<(i16, &'static str)>) -> ConsumerGroupOffsetOutcome {
        ConsumerGroupOffsetOutcome {
            topic: "orders".to_owned(),
            partition,
            error: error.map(|(code, name)| KafkaError {
                code,
                name,
                message: None,
            }),
        }
    }

    #[yare::parameterized(
        all_accepted = { vec![outcome(0, None), outcome(1, None)], &[] },
        one_refused = {
            vec![outcome(0, None), outcome(1, Some((25, "UNKNOWN_MEMBER_ID")))],
            &["orders-1: UNKNOWN_MEMBER_ID (25)"],
        },
        every_refused = {
            vec![
                outcome(0, Some((30, "GROUP_AUTHORIZATION_FAILED"))),
                outcome(1, Some((30, "GROUP_AUTHORIZATION_FAILED"))),
            ],
            &[
                "orders-0: GROUP_AUTHORIZATION_FAILED (30)",
                "orders-1: GROUP_AUTHORIZATION_FAILED (30)",
            ],
        },
    )]
    fn only_the_refused_partitions_are_reported(
        outcomes: Vec<ConsumerGroupOffsetOutcome>,
        expected: &[&str],
    ) {
        check!(commit_refusals(&outcomes) == expected);
    }

    #[yare::parameterized(
        committed = { Ok(vec![outcome(0, None), outcome(1, None)]), Ok(3) },
        refused = {
            Ok(vec![outcome(0, None), outcome(1, Some((25, "UNKNOWN_MEMBER_ID")))]),
            Err("cluster: OffsetCommit for orders-consumers: orders-1: UNKNOWN_MEMBER_ID (25)"),
        },
        coordinator_never_ready = {
            Err(AdminError::Transport(ClientError::Timeout(krabka_units::secs(60)))),
            Err("cluster: OffsetCommit for orders-consumers: client-core: request timed out \
                 after 1m: the group coordinator did not become available. A broker creates \
                 and loads `__consumer_offsets` on the first coordinator lookup; check the \
                 broker log, for example for INVALID_REPLICATION_FACTOR"),
        },
        call_failed = {
            Err(AdminError::Transport(ClientError::Server { error_code: 30 })),
            Err("cluster: OffsetCommit for orders-consumers: client-core: protocol error from \
                 server: 30"),
        },
    )]
    fn a_group_commit_reports_its_count_or_why_it_committed_nothing(
        result: Result<Vec<ConsumerGroupOffsetOutcome>, AdminError>,
        expected: Result<usize, &str>,
    ) {
        let actual = commit_outcome(&captured(), result).map_err(|error| error.to_string());
        check!(actual == expected.map_err(str::to_owned));
    }

    /// The exact bytes this build writes for one group of [`captured`]. A
    /// change here is a change to the 1.x capture format.
    const GOLDEN_OFFSETS: &str = concat!(
        r#"{"version":0,"groups":[{"group":"orders-consumers","offsets":["#,
        r#"{"topic":"orders","partition":0,"offset":12},"#,
        r#"{"topic":"orders","partition":1,"offset":7},"#,
        r#"{"topic":"payments","partition":0,"offset":3}]}]}"#,
    );

    #[test]
    fn an_offsets_file_encodes_to_the_golden_bytes_and_decodes_back() {
        let file = GroupOffsetsFile {
            version: GROUP_OFFSETS_VERSION,
            groups: vec![captured()],
        };
        check!(serde_json::to_string(&file).unwrap() == GOLDEN_OFFSETS);
        check!(
            GroupOffsetsFile::from_slice(GOLDEN_OFFSETS.as_bytes(), "group-offsets.json").unwrap()
                == file
        );
        check!(GroupOffsetsFile::default().version == GROUP_OFFSETS_VERSION);
    }

    #[yare::parameterized(
        pre_1_0 = { "pre-1.0 offsets file", r#"{"groups":[]}"#, None },
        future = { "future version", r#"{"version":1,"groups":[]}"#, Some("1") },
        non_numeric = {
            "version that is not a number",
            r#"{"version":"0","groups":[]}"#,
            Some("\"0\""),
        },
    )]
    fn an_offsets_file_with_a_missing_or_unknown_version_is_refused(
        name: &str,
        json: &str,
        found: Option<&str>,
    ) {
        let error =
            GroupOffsetsFile::from_slice(json.as_bytes(), "group-offsets.json").expect_err(name);
        assert2::assert!(
            let BackupError::UnsupportedVersion {
                found: actual,
                expected: GROUP_OFFSETS_VERSION,
                ..
            } = &error,
            "case {name}: {error}"
        );
        check!(actual.as_deref() == found, "case {name}");
    }

    #[test]
    fn the_offsets_file_counts_every_partition_of_every_group() {
        let file = GroupOffsetsFile {
            version: GROUP_OFFSETS_VERSION,
            groups: vec![captured(), GroupOffsets {
                group: "empty".to_owned(),
                offsets: Vec::new(),
            }],
        };
        check!(file.offset_count() == 3);
    }
}
