//! The two throttles a finished fetch passes: the KIP-73 leader-side
//! replication throttle on a follower fetch, and the KIP-13 consumer byte
//! rate together with the KIP-124 request quota on a client fetch.

use {
    crate::{
        broker::Broker,
        handlers::{ApiKeyCode, RequestContext},
        metrics::{BrokerMetrics, QuotaType},
        quota::{
            QuotaBuckets, QuotaDelay, debt_tokens, entity_field, lookup_quota_with_key,
            throttle_time_ms,
        },
        throttle::{REPLICATION_QUOTA_WINDOW, TokenBucket, TopicThrottle},
    },
    krabka_protocol::{
        Encode as _,
        owned::fetch_response::{FetchResponse, FetchableTopicResponse},
        records::RecordsPayload,
    },
    krabka_units::{Time, convert::TimeExt},
    num_traits::ToPrimitive as _,
};

pub(super) fn throttle_follower_responses(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    responses: &[FetchableTopicResponse],
) {
    record_throttled_replication(
        &broker.throttle_state.leader_out,
        &broker.metrics,
        image,
        broker.config.node_id,
        responses,
    );
}

/// Kafka's `ReplicaManager.shouldLeaderThrottle`, less its quota check: the
/// leader `leader` throttles the read of `partition` of `topic` by the
/// replica `follower_id`.
///
/// A partition is throttled when the leader's own id is on its
/// `leader.replication.throttled.replicas` list, whichever follower fetches it
/// (`ConfigHandler.parseThrottledPartitions` keeps the entries that name the
/// broker itself, and `ReplicationQuotaManager.isThrottled` is per partition,
/// #1210). A follower that is in the partition's ISR is never throttled, to
/// avoid ISR thrashing (#1211).
///
/// The quota check is the read loop's to make, right before it hands the
/// read's records out, as Kafka's `readFromLog` makes it: a row read while
/// the leader-out bucket reports [`TokenBucket::quota_exceeded`] carries no
/// records.
pub(super) fn leader_throttles_replica(
    image: &krabka_metadata::MetadataImage,
    leader: krabka_metadata::NodeId,
    follower_id: i32,
    topic: &str,
    partition: i32,
) -> bool {
    let Ok(follower) = u64::try_from(follower_id) else {
        return false;
    };
    TopicThrottle::for_topic(image, topic, leader)
        .leader
        .contains(partition)
        && !image
            .partition(topic, partition)
            .is_some_and(|record| record.isr.contains(&krabka_metadata::NodeId(follower)))
}

/// Whether the leader withholds the records of a follower read: the leader
/// throttles the replica ([`leader_throttles_replica`]) and `leader_out`
/// reports its quota exceeded right now, Kafka's `isQuotaExceeded`. The read
/// loop asks this of every read it makes, so a fetch that parks finds the
/// quota as it is when it wakes.
pub(super) fn leader_withholds(leader_out: &TokenBucket, replica_throttled: bool) -> bool {
    replica_throttled && leader_out.quota_exceeded()
}

/// Records the bytes a follower fetch sends of every partition that the leader
/// `leader` throttles, in sync or not, as Kafka's `KafkaApis` records
/// `sizeOfThrottledPartitions` once the response is built.
///
/// The read loop has already withheld the records of every out-of-sync
/// partition that it read while the quota was exceeded. The partitions it
/// read within the quota go out whole, so the charge may overshoot the
/// balance: an indivisible batch larger than the whole burst still
/// replicates, and the debt it leaves makes the quota report itself exceeded
/// until the refill repays it. The debt is kept for one replication quota
/// window at most ([`REPLICATION_QUOTA_WINDOW`]), as Kafka's quota forgets a
/// sample that leaves its window. So a burst stalls the throttled replicas for
/// that long at most, and not for as long as the burst was big.
///
/// [`REPLICATION_QUOTA_WINDOW`]: crate::throttle::REPLICATION_QUOTA_WINDOW
fn record_throttled_replication(
    leader_out: &TokenBucket,
    metrics: &BrokerMetrics,
    image: &krabka_metadata::MetadataImage,
    leader: krabka_metadata::NodeId,
    responses: &[FetchableTopicResponse],
) {
    let mut sent = 0_u64;
    for topic in responses {
        let throttle = TopicThrottle::for_topic(image, &topic.topic, leader);
        sent += topic
            .partitions
            .iter()
            .filter(|partition| throttle.leader.contains(partition.partition_index))
            .map(|partition| {
                partition
                    .records
                    .as_ref()
                    .map_or(0, RecordsPayload::payload_len) as u64
            })
            .sum::<u64>();
    }
    if sent == 0 {
        return;
    }
    leader_out.record_bounded(sent, REPLICATION_QUOTA_WINDOW);
    // KIP-73: the measured leader-side throttled-replication rate, which
    // Kafka publishes as
    // `kafka.server:type=LeaderReplication,name=byte-rate`. It is what
    // tells an operator whose reassignment is not moving whether the
    // throttle is biting or something else is wrong.
    metrics.record_replication_throttled_out(sent);
}

/// The bytes a consumer may read in one fetch without a throttle: Kafka's
/// `ClientQuotaManager.maxValueInQuotaWindow`, which is
/// `consumer_byte_rate * (quota.window.num - 1) * quota.window.size.seconds`.
/// `quota_window` is the whole `quota.window.num * quota.window.size.seconds`
/// and `quota_throttle_max` is one `quota.window.size.seconds`. Without a
/// `consumer_byte_rate` the read is not capped.
pub(super) fn consumer_quota_window_bytes(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
) -> usize {
    let Some((_, rate)) = lookup_quota_with_key(
        image,
        &context.principal.name,
        context.client_id,
        "consumer_byte_rate",
    ) else {
        return usize::MAX;
    };
    if !rate.is_finite() || rate <= 0.0 {
        return usize::MAX;
    }
    let window_secs = (broker.config.quota_window - broker.config.quota_throttle_max)
        .secs_f64()
        .max(0.0);
    (rate * window_secs).to_usize().unwrap_or(usize::MAX)
}

/// The byte-rate charge a consumer fetch made, in bytes, which a throttled
/// fetch gives back.
pub(crate) struct ConsumerCharge(Option<ChargedBucket>);

/// The bucket a consumer fetch drew from and the bytes it drew.
struct ChargedBucket {
    bucket: std::sync::Arc<TokenBucket>,
    bytes: u64,
}

impl ConsumerCharge {
    /// Kafka's `quotas.fetch.unrecordQuotaSensor`: a throttled fetch sends
    /// no records, so the bytes it was charged come off the quota. The whole
    /// charge comes back, the debt it left included.
    pub(crate) fn refund(self) {
        if let Some(ChargedBucket { bucket, bytes }) = self.0 {
            bucket.refund(bytes);
        }
    }
}

/// Charge a consumer read whose whole response is `response_bytes` long to
/// its `consumer_byte_rate` and `request_percentage` quotas, and its
/// `handler_time` to the latter, as Kafka's `KafkaApis.handleFetchRequest`
/// records both with `fetchContext.getResponseSize` and
/// `KafkaApis.processShareFetchResponse` with `shareFetchContext.responseSize`.
/// Returns the throttle the `api_key` response reports, and the byte-rate
/// charge to give back when that throttle is above zero.
pub(crate) fn apply_consumer_fetch_quota(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    api_key: ApiKeyCode,
    handler_time: std::time::Duration,
    response_bytes: u64,
) -> (i32, ConsumerCharge) {
    let ConsumerQuotaOutcome {
        throttle: data_delay,
        charge,
    } = consume_consumer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        response_bytes,
    );
    let request_delay = context.charge_request_quota(broker, image, handler_time);
    // KIP-219: the connection is muted for the larger of the two delays.
    // Resolving it through the metric records the throttle phase and the quota
    // that caused it, and hands back the delay the response reports.
    let delay = broker.metrics.record_applied_throttle(api_key, &[
        (QuotaType::Fetch, data_delay).into(),
        (QuotaType::Request, request_delay).into(),
    ]);
    if delay <= <Time as TimeExt>::ZERO {
        return (0, charge);
    }
    // KIP-219: the window goes back to the connection loop, which mutes the
    // connection after the response is written.
    context.record_throttle(delay);
    (throttle_time_ms(delay), charge)
}

/// What Kafka's `FetchContext.getResponseSize` charges a whole Fetch response
/// to the consumer byte rate: four bytes and the encoded `version` body.
pub(super) fn fetch_response_size(response: &FetchResponse, version: i16) -> u64 {
    let size = if version < 4 {
        let legacy: krabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            response.clone().into();
        legacy.encoded_len(version)
    } else {
        response.encoded_len(version)
    };
    // Kafka's `FetchResponse.sizeOf` is `4 + data.size(version)`.
    u64::try_from(size).unwrap_or(u64::MAX).saturating_add(4)
}

/// The throttle a consumer fetch earned from its byte-rate quota, with the
/// charge it made.
struct ConsumerQuotaOutcome {
    throttle: QuotaDelay,
    charge: ConsumerCharge,
}

/// KIP-13 `consumer_byte_rate` enforcement.
///
/// The function looks up the matching quota for `(principal, client_id)`,
/// takes `bytes` from the bucket, and returns the throttle delay with the
/// charge it made. The delay is zero when the config sets no quota, or when
/// the bucket has enough capacity.
fn consume_consumer_quota(
    image: &krabka_metadata::MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    bytes: u64,
) -> ConsumerQuotaOutcome {
    let Some((entity_key, rate)) =
        lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge: ConsumerCharge(None),
        };
    };
    if !rate.is_finite() || rate <= 0.0 {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge: ConsumerCharge(None),
        };
    }
    let user = entity_field(&entity_key, "user");
    let client_id_opt = entity_field(&entity_key, "client-id");
    // Kafka holds the quota as a double (`ClientQuotaManager`), so the bucket
    // runs at the configured rate, fractional part included. Like the
    // producer path it records the whole response and turns the debt into the
    // throttle (#1212); a throttled fetch then gives the whole charge back.
    let bucket = buckets.get_or_create("consumer_byte_rate", &entity_key, rate);
    let debt = bucket.record(bytes);
    let charge = ConsumerCharge(Some(ChargedBucket {
        bucket: std::sync::Arc::clone(&bucket),
        bytes,
    }));
    let Some(overage) = debt_tokens(debt) else {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge,
        };
    };
    let delay_secs = overage / rate;
    // Kafka's `ClientQuotaManager.throttleTime` does not bound a byte-rate
    // throttle.
    let delay = Time::from_secs_f64(delay_secs);
    ConsumerQuotaOutcome {
        throttle: QuotaDelay::new(delay, user, client_id_opt),
        charge,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{ConsumerQuotaOutcome, fetch_response_size},
        crate::{
            handlers::test_support::replicated_partition,
            metrics::BrokerMetrics,
            quota::{
                QuotaBuckets,
                test_support::{QuotaEntityInput, check_fractional_bandwidth, image_with_quota},
            },
            throttle::{LEADER_THROTTLED_REPLICAS_KEY, TokenBucket},
        },
        assert2::{assert, check},
        krabka_units::{Time, convert::TimeExt, millis, secs},
    };

    /// Kafka's `FetchResponse.sizeOf`, which `FetchContext.getResponseSize`
    /// charges to the consumer byte rate, is `4 + data.size(version)`. An
    /// empty response's body is the response array (4 bytes at v0), then
    /// `ThrottleTimeMs` from v1, `ErrorCode` and `SessionId` from v7, and the
    /// compact encoding with its tagged-field byte from v12.
    #[yare::parameterized(
        v0 = { 0, 8 },
        v1 = { 1, 12 },
        v4 = { 4, 12 },
        v7 = { 7, 18 },
        v12 = { 12, 16 },
    )]
    fn an_empty_fetch_response_is_charged_kafkas_size(version: i16, size: u64) {
        let response = krabka_protocol::owned::fetch_response::FetchResponse::default();
        assert2::assert!(fetch_response_size(&response, version) == size);
    }

    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
        let img = image_with_quota()
            .entity(vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(Some("app-x")),
            ])
            .key("consumer_byte_rate")
            .value(1024.0)
            .call();
        // A one-second window, so 4096 bytes at 1024 B/s is over the burst
        // rather than inside the default 11-second one.
        let buckets = QuotaBuckets::with_window(secs(1));
        // 3072 bytes over at 1024 B/s is three seconds, reported whole: Kafka
        // does not bound a byte-rate throttle (#709).
        let delay_match =
            super::consume_consumer_quota(&img, &buckets, "alice", Some("app-x"), 4096).throttle;
        assert!(
            delay_match > millis(2_900) && delay_match <= secs(3),
            "tuple quota match should throttle for the whole overage; got {delay_match:?}"
        );
        let buckets2 = QuotaBuckets::with_window(secs(1));
        let delay_other =
            super::consume_consumer_quota(&img, &buckets2, "alice", Some("other"), 4096).throttle;
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    /// A fetch is charged in full and, when that throttles it, the whole
    /// charge comes back (`ClientQuotaManager.unrecordQuotaSensor`), the debt
    /// it left included: the fetch that follows the throttle finds the bucket
    /// as it was. A throttled fetch that is not refunded stays charged.
    #[test]
    fn a_throttled_fetch_gives_the_whole_charge_back() {
        let img = image_with_quota().key("consumer_byte_rate").call();
        let consume = |buckets: &QuotaBuckets, bytes| {
            super::consume_consumer_quota(&img, buckets, "alice", Some("app"), bytes)
        };

        let refunded = QuotaBuckets::with_window(secs(1));
        let ConsumerQuotaOutcome { throttle, charge } = consume(&refunded, 1_500);
        charge.refund();
        let after_refund = consume(&refunded, 1_000).throttle;

        let kept = QuotaBuckets::with_window(secs(1));
        let _ = consume(&kept, 1_500);
        let after_kept = consume(&kept, 1_000).throttle;

        assert!(throttle > <Time as TimeExt>::ZERO, "{throttle:?}");
        assert!(after_refund == <Time as TimeExt>::ZERO, "{after_refund:?}");
        assert!(after_kept > millis(1_400), "{after_kept:?}");
    }

    /// Kafka's `ClientQuotaManager` holds `consumer_byte_rate` as a double,
    /// so a fractional rate throttles a fetch at that rate: the shortfall
    /// over the rate, neither unbounded nor rounded to a whole byte per
    /// second.
    #[test]
    fn a_fractional_consumer_byte_rate_throttles() {
        // `(consumer_byte_rate, response bytes, expected throttle)`. The
        // one-second window gives the bucket a burst of exactly its rate.
        check_fractional_bandwidth("consumer_byte_rate", |image, buckets, bytes| {
            super::consume_consumer_quota(image, buckets, "alice", Some("app"), bytes)
                .throttle
                .delay
        });
    }

    /// The image of leader broker 1 with topic `t`, whose partition `i` has
    /// the follower, broker 2, in its ISR when `isr_has_follower[i]` says so,
    /// and the given leader-side throttled-replicas list.
    fn leader_image(list: &str, isr_has_follower: &[bool]) -> krabka_metadata::MetadataImage {
        use krabka_metadata::{
            MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
        };
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: i32::try_from(isr_has_follower.len()).expect("few partitions"),
            replication_factor: 2,
        }));
        for (partition, in_sync) in (0..).zip(isr_has_follower) {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                isr: if *in_sync {
                    vec![NodeId(1), NodeId(2)]
                } else {
                    vec![NodeId(1)]
                },
                ..replicated_partition()
                    .topic("t")
                    .partition(partition)
                    .leader(1)
                    .replicas(&[NodeId(1), NodeId(2)])
                    .call()
            }));
        }
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [(LEADER_THROTTLED_REPLICAS_KEY.to_owned(), list.to_owned())]
                .into_iter()
                .collect(),
        }));
        image
    }

    /// One partition of a follower fetch: the bytes of records it carries and
    /// whether the follower is in its ISR.
    #[derive(Clone, Copy)]
    struct FetchedPartition {
        bytes: usize,
        in_sync: bool,
    }

    fn synced(bytes: usize) -> FetchedPartition {
        FetchedPartition {
            bytes,
            in_sync: true,
        }
    }

    fn unsynced(bytes: usize) -> FetchedPartition {
        FetchedPartition {
            bytes,
            in_sync: false,
        }
    }

    /// Runs the leader-side throttle of the follower fetch of broker 2 over
    /// one partition per entry of `partitions`, each with that many bytes of
    /// records and with the follower in its ISR or not: the quota check each
    /// read makes, and the charge the response makes. Returns the bytes each
    /// row still carries.
    fn throttle_leader_fetch(
        leader_out: &TokenBucket,
        list: &str,
        partitions: &[FetchedPartition],
    ) -> Vec<usize> {
        use krabka_protocol::{
            owned::fetch_response::{FetchableTopicResponse, PartitionData},
            records::RecordsPayload,
        };
        let in_isr: Vec<bool> = partitions.iter().map(|row| row.in_sync).collect();
        let image = leader_image(list, &in_isr);
        let leader = krabka_metadata::NodeId(1);
        // The read loop's half: each read asks the quota before its records
        // go out, so every row of one fetch sees the same answer.
        let responses = vec![FetchableTopicResponse {
            topic: "t".into(),
            partitions: (0..)
                .zip(partitions)
                .map(|(partition_index, row)| PartitionData {
                    partition_index,
                    records: (!super::leader_withholds(
                        leader_out,
                        super::leader_throttles_replica(&image, leader, 2, "t", partition_index),
                    ))
                    .then(|| RecordsPayload::Raw(bytes::Bytes::from(vec![0_u8; row.bytes]))),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }];

        super::record_throttled_replication(
            leader_out,
            &BrokerMetrics::new(),
            &image,
            leader,
            &responses,
        );

        responses[0]
            .partitions
            .iter()
            .map(|partition| {
                partition
                    .records
                    .as_ref()
                    .map_or(0, RecordsPayload::payload_len)
            })
            .collect()
    }

    fn leader_bucket(rate: u32) -> TokenBucket {
        let bucket = TokenBucket::new();
        bucket.set_byte_rate(krabka_units::bytes_per_sec(rate));
        bucket
    }

    /// A bucket of `rate` that has recorded its one-second burst and one
    /// second more, so its quota is exceeded for the second the refill takes
    /// to repay the debt.
    fn spent_bucket(rate: u32) -> TokenBucket {
        let bucket = leader_bucket(rate);
        bucket.record(2 * u64::from(rate));
        bucket
    }

    /// A partition is throttled on the leader when the leader's own id is on
    /// its `leader.replication.throttled.replicas` list, whichever follower
    /// fetches it (#1210): the list `kafka-reassign-partitions --throttle`
    /// writes names the source replicas, and never the destination that
    /// fetches. Each row is one fetch by broker 2, an out-of-sync follower,
    /// while the quota is exceeded.
    #[test]
    fn a_leader_throttles_the_partitions_that_name_its_own_id() {
        // (label, list, bytes each partition keeps)
        let cases = [
            ("the leader's id is listed", "0:1,1:1", vec![0, 0]),
            (
                "the follower's id is listed, which is the inverted reading: no throttle",
                "0:2,1:2",
                vec![300, 300],
            ),
            ("only the listed partition is throttled", "1:1", vec![
                300, 0,
            ]),
            ("the wildcard throttles every partition", "*", vec![0, 0]),
            ("no list throttles nothing", "", vec![300, 300]),
        ];
        for (label, list, want) in cases {
            check!(
                throttle_leader_fetch(&spent_bucket(500), list, &[unsynced(300), unsynced(300)])
                    == want,
                "{label}"
            );
        }
    }

    /// A replica id below zero is no broker, so no list names it.
    #[test]
    fn a_fetch_without_a_broker_id_is_not_throttled() {
        let image = leader_image("*", &[false]);

        check!(!super::leader_throttles_replica(
            &image,
            krabka_metadata::NodeId(1),
            -1,
            "t",
            0
        ));
    }

    /// An indivisible batch larger than the whole one-second burst replicates
    /// once the quota is not exceeded, as Kafka checks the quota before the
    /// read and records the read's size after it. The overshoot is debt, and
    /// the replica is held back until the refill has repaid it, so the
    /// long-term rate holds (#1340).
    #[test]
    fn an_oversized_batch_replicates_and_its_overshoot_is_paid_back() {
        let clock = qubit_clock::ManualMonotonicClock::new_shared();
        let bucket = TokenBucket::with_clock(clock.clone());
        bucket.set_byte_rate(krabka_units::bytes_per_sec(500));
        let fetch_after = |millis| {
            clock
                .advance(std::time::Duration::from_millis(millis))
                .expect("manual time moves forward");
            throttle_leader_fetch(&bucket, "*", &[unsynced(800)])
        };

        // 800 bytes leave 300 bytes of debt, which 500 B/s repays in 600 ms.
        let rounds = [
            fetch_after(0),
            fetch_after(0),
            fetch_after(599),
            fetch_after(2),
        ];

        check!(rounds == [vec![800], vec![0], vec![0], vec![800]]);
    }

    /// A follower in the ISR is never throttled, however far over the quota
    /// the leader is (#1211).
    #[test]
    fn an_in_sync_follower_is_never_throttled() {
        let kept = throttle_leader_fetch(&spent_bucket(100), "*", &[synced(5_000), synced(300)]);

        check!(kept == vec![5_000, 300]);
    }

    /// The bytes an in-sync follower reads still count against the quota
    /// (`KafkaApis.sizeOfThrottledPartitions`), so a burst of them leaves the
    /// bucket in debt and an out-of-sync replica that fetches next is held
    /// back until the debt is repaid (#1211).
    #[test]
    fn in_sync_bytes_are_charged_against_the_leader_quota() {
        let bucket = leader_bucket(1_000);

        // The quota is not exceeded when the fetch reads, so both partitions
        // go out, and their 4600 bytes are then recorded.
        let mixed = throttle_leader_fetch(&bucket, "*", &[synced(4_000), unsynced(600)]);
        let next = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);

        check!((mixed, next) == (vec![4_000, 600], vec![0]));
    }

    /// The debt an in-sync burst leaves is kept for one replication quota
    /// window at most, as Kafka's quota forgets a sample that leaves its
    /// window. Without the bound a lagging follower on a throttled partition
    /// is starved for as long as the burst was big, drops out of the ISR, and
    /// `acks=all` produces fail with `NOT_ENOUGH_REPLICAS`. The burst here is
    /// `100_000` bytes on a `1_000` B/s quota: 99 s of debt unbounded, 11 s bounded.
    #[test]
    fn an_in_sync_burst_is_forgotten_after_the_replication_window() {
        let clock = qubit_clock::ManualMonotonicClock::new_shared();
        let bucket = TokenBucket::with_clock(clock.clone());
        bucket.set_byte_rate(krabka_units::bytes_per_sec(1_000));

        throttle_leader_fetch(&bucket, "*", &[synced(100_000)]);
        let during = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);
        clock
            .advance(std::time::Duration::from_secs(12))
            .expect("manual time moves forward");
        let after = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);

        check!((during, after) == (vec![0], vec![600]));
    }

    /// The quota is one answer for the whole fetch: within it, every throttled
    /// partition goes out whole, even past what the bucket holds, and none is
    /// cut to fit. The next fetch pays for the overshoot.
    #[test]
    fn a_fetch_within_the_quota_goes_out_whole() {
        let bucket = leader_bucket(500);

        let first = throttle_leader_fetch(&bucket, "*", &[unsynced(400), unsynced(400)]);
        let second = throttle_leader_fetch(&bucket, "*", &[unsynced(100)]);

        check!((first, second) == (vec![400, 400], vec![0]));
    }

    /// A rate of zero is no throttle, in sync or not.
    #[test]
    fn an_unthrottled_leader_leaves_every_partition_alone() {
        let kept = throttle_leader_fetch(&TokenBucket::new(), "*", &[unsynced(800), synced(800)]);

        check!(kept == vec![800, 800]);
    }
}
