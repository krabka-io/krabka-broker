//! Validation of one WAL fetch response. The follower trusts nothing the leader
//! sends, so the frontiers, the reset offset, and the offset the append reached
//! each pass through a check here before the follower acts on them.

use {
    crate::wal::quorum::registry::ShardId,
    krabka_ids::Offset,
    krabka_kraft_core::LogOffsetMetadata,
    krabka_protocol::{
        owned::fetch_response::{FetchResponse, PartitionData},
        primitives::uuid::Uuid,
    },
    std::cmp,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalFetchFrontiers {
    pub(super) start: Offset,
    pub(super) end: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchProgress {
    Idle,
    Advanced,
}

pub(super) fn validate_fetch_frontiers(
    partition: &PartitionData,
) -> Result<WalFetchFrontiers, String> {
    let start = Offset(partition.log_start_offset);
    let high_watermark = Offset(partition.high_watermark);
    let end = Offset(partition.last_stable_offset);
    let (true, true) = (
        (Offset(0)..=end).contains(&start),
        (start..=end).contains(&high_watermark),
    ) else {
        return Err("leader returned invalid WAL frontiers".into());
    };
    Ok(WalFetchFrontiers { start, end })
}

pub(super) fn validate_reset_offset(partition: &PartitionData) -> Result<Offset, String> {
    let start = Offset(partition.log_start_offset);
    let end = Offset(partition.last_stable_offset);
    let true = (Offset(0)..=end).contains(&start) else {
        return Err("leader returned invalid WAL reset offset".into());
    };
    Ok(start)
}

/// The diverging epoch a response names, or `None` when it names none.
///
/// Kafka's `FetchResponseData.EpochEndOffset` defaults to epoch `-1` and end
/// offset `-1`, and a leader leaves it at that default for a consistent
/// fetch, so a negative end offset means "no divergence". A named divergence
/// must carry a real epoch.
pub(super) fn validate_diverging_epoch(
    partition: &PartitionData,
) -> Result<Option<LogOffsetMetadata>, String> {
    let diverging = &partition.diverging_epoch;
    if diverging.end_offset < 0 {
        return Ok(None);
    }
    let epoch = u32::try_from(diverging.epoch)
        .map_err(|_| String::from("leader returned an invalid WAL diverging epoch"))?;
    Ok(Some(LogOffsetMetadata {
        offset: diverging.end_offset,
        epoch,
    }))
}

pub(super) fn fetch_progress(requested: Offset, appended: Offset) -> Result<FetchProgress, String> {
    match appended.cmp(&requested) {
        cmp::Ordering::Less => Err(format!(
            "WAL follower regressed from requested offset {} to {}",
            requested.0, appended.0
        )),
        cmp::Ordering::Equal => Ok(FetchProgress::Idle),
        cmp::Ordering::Greater => Ok(FetchProgress::Advanced),
    }
}

pub(super) fn response_partition(response: FetchResponse, shard: ShardId) -> Option<PartitionData> {
    let topic_id = Uuid(*shard.topic_id.as_bytes());
    response
        .responses
        .into_iter()
        .find(|topic| topic.topic_id == topic_id)?
        .partitions
        .into_iter()
        .find(|partition| partition.partition_index == shard.partition)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn frontiers(start: i64, high_watermark: i64, end: i64) -> PartitionData {
        PartitionData {
            log_start_offset: start,
            high_watermark,
            last_stable_offset: end,
            ..Default::default()
        }
    }

    #[yare::parameterized(
        valid = { frontiers(5, 6, 7), true },
        negative_start = { frontiers(-1, 0, 1), false },
        negative_high_watermark = { frontiers(0, -1, 1), false },
        high_watermark_after_end = { frontiers(0, 2, 1), false },
        start_after_end = { frontiers(2, 2, 1), false },
        negative_end = { frontiers(0, 0, -1), false },
    )]
    fn fetch_frontiers_require_an_ordered_nonnegative_range(data: PartitionData, valid: bool) {
        let result = validate_fetch_frontiers(&data);
        if valid {
            assert2::assert!(
                result
                    == Ok(WalFetchFrontiers {
                        start: Offset(5),
                        end: Offset(7),
                    })
            );
        } else {
            assert2::assert!(result.is_err());
        }
    }

    #[yare::parameterized(
        at_log_start = { frontiers(5, 5, 7), true },
        negative_start = { frontiers(-1, 0, 7), false },
        start_after_end = { frontiers(8, 8, 7), false },
        negative_end = { frontiers(0, 0, -1), false },
    )]
    fn reset_offset_must_fall_inside_the_leader_log(data: PartitionData, valid: bool) {
        let result = validate_reset_offset(&data);
        if valid {
            assert2::assert!(result == Ok(Offset(5)));
        } else {
            assert2::assert!(result.is_err());
        }
    }

    #[derive(Default)]
    struct WalDivergenceResponse {
        partition: Option<PartitionData>,
        validated: Option<Result<Option<LogOffsetMetadata>, String>>,
    }

    #[rstest::fixture]
    fn wal_divergence_response() -> WalDivergenceResponse {
        // Each leader response has its own advertised epoch and validation result.
        WalDivergenceResponse::default()
    }

    #[rstest_bdd_macros::given("the WAL leader advertises diverging epoch {epoch} ending at {end}")]
    fn supplied_wal_divergence_response(
        wal_divergence_response: &mut WalDivergenceResponse,
        epoch: i32,
        end: i64,
    ) {
        let mut partition = frontiers(0, 0, 10);
        partition.diverging_epoch.epoch = epoch;
        partition.diverging_epoch.end_offset = end;
        wal_divergence_response.partition = Some(partition);
    }

    #[rstest_bdd_macros::when("the WAL follower validates the leader's diverging epoch")]
    fn validate_wal_divergence_response(wal_divergence_response: &mut WalDivergenceResponse) {
        wal_divergence_response.validated = Some(validate_diverging_epoch(
            wal_divergence_response
                .partition
                .as_ref()
                .expect("leader partition response was given"),
        ));
    }

    #[rstest_bdd_macros::then("the validated WAL divergence is absent")]
    fn absent_wal_divergence_response(wal_divergence_response: &WalDivergenceResponse) {
        assert2::assert!(wal_divergence_response.validated == Some(Ok(None)));
    }

    #[rstest_bdd_macros::then("the validated WAL divergence is epoch {epoch} at offset {end}")]
    fn present_wal_divergence_response(
        wal_divergence_response: &WalDivergenceResponse,
        epoch: i32,
        end: i64,
    ) {
        assert2::assert!(
            wal_divergence_response.validated
                == Some(Ok(Some(LogOffsetMetadata {
                    offset: end,
                    epoch: epoch.try_into().expect("nonnegative validated epoch")
                })))
        );
    }

    #[rstest_bdd_macros::then("WAL divergence validation fails with {message}")]
    fn refused_wal_divergence_response(
        wal_divergence_response: &WalDivergenceResponse,
        message: String,
    ) {
        assert2::assert!(wal_divergence_response.validated == Some(Err(message)));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/wal/quorum/follower/fetch.feature",
        fixtures = [wal_divergence_response: WalDivergenceResponse]
    );

    #[test]
    fn fetch_progress_distinguishes_idle_advance_and_regression() {
        assert!(fetch_progress(Offset(4), Offset(4)) == Ok(FetchProgress::Idle));
        assert!(fetch_progress(Offset(4), Offset(5)) == Ok(FetchProgress::Advanced));
        assert!(fetch_progress(Offset(4), Offset(3)).is_err());
    }
}
