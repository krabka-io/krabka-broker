//! KIP-1023: an empty follower of a tiered partition starts at the leader's
//! earliest pending upload offset rather than at its local log start.
//!
//! Kafka gates the behaviour on the dynamic broker config
//! `follower.fetch.last.tiered.offset.enable`, which defaults to `false`.
//! `ReplicaFetcherThread.shouldFetchFromLastTieredOffset` reads it on every
//! decision, so a change applies to the next recovery without a restart. This
//! module reads it from the metadata image in the same way.
//!
//! The leader answers the earliest pending upload offset to
//! `ListOffsets(EARLIEST_PENDING_UPLOAD_TIMESTAMP)`, at version 11 and later.
//! It is the first offset above the last segment the leader has copied to the
//! remote tier, and never below the log start. Every offset below it is in the
//! archive already, so an empty follower that starts there replicates only
//! what is still waiting for an upload. Without the flag the follower starts
//! at the leader's local log start and replicates every local segment, tiered
//! or not.

use {
    super::EARLIEST_LOCAL_TIMESTAMP,
    crate::{
        config_keys::{node_or_cluster_default, parse::bool_value},
        partition::Partition,
        replicator::Config,
    },
};

/// Kafka's `ReplicationConfigs.FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE_CONFIG`.
const FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE: &str = "follower.fetch.last.tiered.offset.enable";

/// Kafka's `ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP` (KIP-1023):
/// the first offset the leader has not yet copied to the remote tier.
pub(super) const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

/// The offset a leader reports for `EARLIEST_PENDING_UPLOAD_TIMESTAMP` when it
/// cannot name one: the topic is not tiered, or segments were evicted locally
/// on the strength of an upload this leader cannot confirm.
pub(super) const UNKNOWN_PENDING_UPLOAD_OFFSET: i64 = -1;

/// `follower.fetch.last.tiered.offset.enable` as this broker sees it now: its
/// own dynamic value, then the cluster default, then Kafka's default `false`.
///
/// A value is read the way Kafka's `ConfigDef` reads a `BOOLEAN`: `true` or
/// `false` in any case, after a trim. Validation refuses anything else, so a
/// value that does not parse reads as the default.
pub(super) fn follower_fetch_last_tiered_offset_enabled(cfg: &Config) -> bool {
    let image = cfg.controller.current_image();
    node_or_cluster_default(
        &image,
        cfg.node_id,
        FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE,
    )
    .and_then(bool_value)
    .unwrap_or(false)
}

/// The facts `ReplicaFetcherThread.shouldFetchFromLastTieredOffset` decides
/// on.
#[derive(Clone, Copy, Debug)]
pub(super) struct LastTieredOffsetFacts {
    /// `follower.fetch.last.tiered.offset.enable`.
    pub(super) enabled: bool,
    /// This replica's own `remote.storage.enable` for the topic.
    pub(super) remote_storage_enable: bool,
    /// Whether this replica's `cleanup.policy` contains `compact`.
    pub(super) compact: bool,
    /// This replica's log end offset.
    pub(super) replica_end: i64,
    /// The leader's log end offset.
    pub(super) leader_end: i64,
}

/// The local half of [`LastTieredOffsetFacts`]: everything but the leader's
/// log end, which costs a round trip.
#[derive(Clone, Copy, Debug)]
pub(super) struct LocalTieringFacts {
    pub(super) enabled: bool,
    pub(super) remote_storage_enable: bool,
    pub(super) compact: bool,
    pub(super) replica_end: i64,
}

impl LocalTieringFacts {
    /// Reads the flag from the metadata image and the rest from `partition`'s
    /// log, under one log mutex hold.
    pub(super) fn read(cfg: &Config, partition: &Partition) -> Self {
        let enabled = follower_fetch_last_tiered_offset_enabled(cfg);
        let log = partition.log.lock().expect("log mutex poisoned");
        let config = log.config_snapshot();
        Self {
            enabled,
            remote_storage_enable: config.remote_storage_enable,
            compact: config.cleanup_policy.contains_compact(),
            replica_end: log.log_end_offset().0,
        }
    }

    /// Whether the decision can still come out `true` for some leader log end.
    ///
    /// A caller that would ask the leader for its log end only to make this
    /// decision skips the round trip when it cannot.
    pub(super) fn may_fetch_from_last_tiered_offset(self) -> bool {
        self.enabled && self.remote_storage_enable && !self.compact && self.replica_end == 0
    }

    /// The whole decision, once the leader's log end is known.
    pub(super) fn with_leader_end(self, leader_end: i64) -> LastTieredOffsetFacts {
        LastTieredOffsetFacts {
            enabled: self.enabled,
            remote_storage_enable: self.remote_storage_enable,
            compact: self.compact,
            replica_end: self.replica_end,
            leader_end,
        }
    }
}

/// `ReplicaFetcherThread.shouldFetchFromLastTieredOffset`: an empty replica of
/// a tiered, non-compacted topic, with the flag set, whose leader holds at
/// least one offset.
pub(super) fn should_fetch_from_last_tiered_offset(facts: LastTieredOffsetFacts) -> bool {
    let LastTieredOffsetFacts {
        enabled,
        remote_storage_enable,
        compact,
        replica_end,
        leader_end,
    } = facts;
    enabled && remote_storage_enable && !compact && replica_end == 0 && leader_end != 0
}

/// `AbstractFetcherThread.fetchEarliestPendingUploadOffset`: where an empty
/// follower starts when [`should_fetch_from_last_tiered_offset`] holds.
///
/// `leader_offset` asks the leader for its offset at a `ListOffsets` sentinel.
/// For `EARLIEST_PENDING_UPLOAD_TIMESTAMP` it passes the leader's
/// [`UNKNOWN_PENDING_UPLOAD_OFFSET`] through rather than failing on it.
///
/// When the leader cannot name a pending upload offset, the follower asks for
/// the leader's local log start. If that is the leader's log start, nothing
/// has been evicted and the follower starts there. Otherwise segments were
/// uploaded but the leader does not know which, and Kafka throws
/// `OffsetNotAvailableException`; this returns an error, and the caller backs
/// off and asks again.
pub(super) async fn earliest_pending_upload_offset<F, Fut>(
    leader_offset: &mut F,
    leader_log_start: i64,
) -> Result<i64, String>
where
    F: FnMut(i64) -> Fut,
    Fut: Future<Output = Result<i64, String>>,
{
    let pending = leader_offset(EARLIEST_PENDING_UPLOAD_TIMESTAMP).await?;
    if pending != UNKNOWN_PENDING_UPLOAD_OFFSET {
        return Ok(pending);
    }
    let leader_local_start = leader_offset(EARLIEST_LOCAL_TIMESTAMP).await?;
    if leader_local_start == leader_log_start {
        Ok(leader_local_start)
    } else {
        Err(
            "segments are uploaded to remote storage, but the leader does not know the earliest \
             pending upload offset"
                .to_owned(),
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::replicator::test_support::{LEADER_ID, NODE_ID, image_with_leader, test_config},
        assert2::check,
        krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
        },
    };

    fn facts(
        enabled: bool,
        remote_storage_enable: bool,
        compact: bool,
        replica_end: i64,
        leader_end: i64,
    ) -> LastTieredOffsetFacts {
        LastTieredOffsetFacts {
            enabled,
            remote_storage_enable,
            compact,
            replica_end,
            leader_end,
        }
    }

    /// One row per condition of `shouldFetchFromLastTieredOffset`, each the
    /// all-true case with that one condition flipped.
    #[yare::parameterized(
        every_condition_holds = { facts(true, true, false, 0, 42), true },
        flag_off = { facts(false, true, false, 0, 42), false },
        not_tiered = { facts(true, false, false, 0, 42), false },
        compacted = { facts(true, true, true, 0, 42), false },
        follower_not_empty = { facts(true, true, false, 7, 42), false },
        leader_empty = { facts(true, true, false, 0, 0), false },
        flag_off_and_not_tiered = { facts(false, false, false, 0, 42), false },
    )]
    fn the_decision_follows_should_fetch_from_last_tiered_offset(
        facts: LastTieredOffsetFacts,
        expected: bool,
    ) {
        check!(should_fetch_from_last_tiered_offset(facts) == expected);
        let local = LocalTieringFacts {
            enabled: facts.enabled,
            remote_storage_enable: facts.remote_storage_enable,
            compact: facts.compact,
            replica_end: facts.replica_end,
        };
        // The local pre-check never rules out a case the whole decision takes.
        check!(local.may_fetch_from_last_tiered_offset() || !expected);
        check!(
            should_fetch_from_last_tiered_offset(local.with_leader_end(facts.leader_end))
                == expected
        );
    }

    fn broker_config(node_id: NodeId, value: &str) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE.to_owned(),
            config_value: Some(value.to_owned()),
        })
    }

    /// This broker's own value wins over the cluster default, which wins over
    /// Kafka's `false`. A value is a Kafka `BOOLEAN`, and one that does not
    /// parse reads as the default.
    #[yare::parameterized(
        nothing_published = { None, None, None, false },
        cluster_default_on = { Some("true"), None, None, true },
        cluster_default_any_case = { Some(" TRUE "), None, None, true },
        cluster_default_off = { Some("false"), None, None, false },
        this_broker_on = { None, Some("true"), None, true },
        this_broker_beats_cluster_on = { Some("true"), Some("false"), None, false },
        this_broker_beats_cluster_off = { Some("false"), Some("True"), None, true },
        another_broker_only = { None, None, Some("true"), false },
        unparseable = { Some("yes"), None, None, false },
    )]
    fn the_flag_is_this_broker_then_the_cluster_default_then_false(
        cluster_default: Option<&str>,
        this_broker: Option<&str>,
        other_broker: Option<&str>,
        expected: bool,
    ) {
        let mut image = image_with_leader(LEADER_ID);
        for (node_id, value) in [
            (DEFAULT_BROKER_CONFIG_NODE_ID, cluster_default),
            (NODE_ID, this_broker),
            (LEADER_ID, other_broker),
        ] {
            if let Some(value) = value {
                image.apply(&broker_config(node_id, value));
            }
        }
        let (cfg, _log_dir) = test_config(image);

        check!(follower_fetch_last_tiered_offset_enabled(&cfg) == expected);
    }

    /// What one `fetchEarliestPendingUploadOffset` did: the sentinels it asked
    /// the leader for, in order, and its answer.
    #[derive(Debug, PartialEq)]
    struct PendingUpload {
        asked: Vec<i64>,
        answer: Result<i64, String>,
    }

    /// `fetchEarliestPendingUploadOffset` over a leader whose log starts at 10
    /// and whose answers are scripted per sentinel.
    #[yare::parameterized(
        the_leader_names_the_offset = {
            Ok(57), Ok(30), PendingUpload { asked: vec![-6], answer: Ok(57) }
        },
        nothing_uploaded_or_evicted = {
            Ok(-1), Ok(10),
            PendingUpload { asked: vec![-6, -4], answer: Ok(10) }
        },
        evicted_but_the_uploads_are_unknown = {
            Ok(-1), Ok(30),
            PendingUpload {
                asked: vec![-6, -4],
                answer: Err(
                    "segments are uploaded to remote storage, but the leader does not know \
                     the earliest pending upload offset"
                        .to_owned()
                ),
            }
        },
        the_pending_lookup_fails = {
            Err("down".to_owned()), Ok(10),
            PendingUpload { asked: vec![-6], answer: Err("down".to_owned()) }
        },
        the_local_start_lookup_fails = {
            Ok(-1), Err("down".to_owned()),
            PendingUpload { asked: vec![-6, -4], answer: Err("down".to_owned()) }
        },
    )]
    #[test_macro(tokio::test)]
    async fn the_start_offset_follows_fetch_earliest_pending_upload_offset(
        pending: Result<i64, String>,
        local_start: Result<i64, String>,
        expected: PendingUpload,
    ) {
        let mut asked = Vec::new();
        let answer = earliest_pending_upload_offset(
            &mut |timestamp| {
                asked.push(timestamp);
                std::future::ready(match timestamp {
                    EARLIEST_PENDING_UPLOAD_TIMESTAMP => pending.clone(),
                    EARLIEST_LOCAL_TIMESTAMP => local_start.clone(),
                    other => Err(format!("unexpected sentinel {other}")),
                })
            },
            10,
        )
        .await;

        check!(PendingUpload { asked, answer } == expected);
    }
}
