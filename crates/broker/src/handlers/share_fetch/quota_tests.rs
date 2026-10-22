//! Handler tests for how a `ShareFetch` is charged to the member's client
//! quotas.
//!
//! Kafka's `KafkaApis.processShareFetchResponse` records the response size
//! against `consumer_byte_rate` and the handler time against
//! `request_percentage`. When either quota is violated it takes the byte
//! charge back, answers with no fetch rows and the larger of the two
//! throttles, and leaves the records it acquired locked until their
//! acquisition lock runs out. The piggybacked acknowledgements of a throttled
//! fetch still apply and still answer.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::byte_limit_tests::{
        RECORDS_PER_BATCH, acquired, batch_size, create_topic, produce_batch,
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            OffsetRun, set_user_quota, share_fetch_partition_rows, share_fetch_wire,
            share_record_states,
        },
        share_partition::state::RecordState::Acquired,
        test_support::{
            initialize_share_state, start_broker_no_audit_with, tolerate_starved_heartbeats,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            share_fetch_request::{AcknowledgementBatch, MAX_VERSION},
            share_fetch_response::{
                PartitionData, ShareFetchResponse, ShareFetchableTopicResponse,
            },
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordsPayload,
    },
    krabka_units::secs,
    std::sync::Arc,
};

/// The member every case fetches as.
const MEMBER: &str = "member";

/// The acknowledge type `Accept`.
const ACCEPT: i8 = 1;

/// A `request_percentage` so small that any handler time exceeds it.
const STARVED_REQUEST_PERCENTAGE: f64 = 0.0001;

/// The broker's quota window, in seconds. A byte-rate bucket holds this many
/// seconds of its rate. The window is long, so the bucket refills by a
/// negligible part of a batch between two fetches of a case.
const QUOTA_WINDOW_SECS: u32 = 20;

/// A broker with a [`QUOTA_WINDOW_SECS`] quota window.
async fn start() -> HandlerStartedBroker {
    start_broker_no_audit_with(|cfg| {
        tolerate_starved_heartbeats(cfg);
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.quota_window = secs(QUOTA_WINDOW_SECS);
        cfg.quota_throttle_max = secs(QUOTA_WINDOW_SECS / 2);
    })
    .await
}

/// The client quota of the member's user.
#[derive(Clone, Copy, Debug, PartialEq)]
enum MemberQuota {
    None,
    /// The `consumer_byte_rate` whose bucket holds this many batches.
    BatchesPerWindow(f64),
    /// `request_percentage`.
    RequestPercentage(f64),
}

/// One `ShareFetch` of `MEMBER` in `group`, which asks for one batch unless
/// it names `max_records`.
#[bon::builder]
async fn fetch(
    broker: &BrokerHandle,
    group: &str,
    user: &str,
    topic_id: WireUuid,
    epoch: i32,
    #[builder(default = i32::try_from(RECORDS_PER_BATCH).expect("small batch"))] max_records: i32,
    #[builder(default)] acknowledgement_batches: Vec<AcknowledgementBatch>,
) -> ShareFetchResponse {
    let request = super::test_fetch_request()
        .group_id(group.into())
        .member_id(MEMBER.into())
        .topic_id(topic_id.0)
        .share_session_epoch(epoch)
        .max_records(max_records)
        .acknowledgement_batches(acknowledgement_batches)
        .call();
    share_fetch_wire()
        .broker(broker)
        .request(&request)
        .version(MAX_VERSION)
        .user(user)
        .call()
        .await
}

/// A topic of two batches, with `MEMBER`'s share session in `group` open on it.
async fn open_session(broker: &BrokerHandle, name: &str, group: &str, user: &str) -> WireUuid {
    let topic_id = create_topic(broker, name).await;
    initialize_share_state(broker, group, uuid::Uuid::from_bytes(topic_id.0), 0).await;
    let opened = fetch()
        .broker(broker)
        .group(group)
        .user(user)
        .topic_id(topic_id)
        .epoch(0)
        .call()
        .await;
    assert!(opened.error_code == codes::NONE, "{opened:?}");
    for _ in 0..2 {
        produce_batch(broker, name, 0).await;
    }
    topic_id
}

/// Sets the `consumer_byte_rate` of `user` so that its bucket holds `batches`
/// batches of `topic`.
async fn set_batches_per_window(broker: &BrokerHandle, topic: &str, user: &str, batches: f64) {
    let rate = f64::from(batch_size(broker, topic)) * batches / f64::from(QUOTA_WINDOW_SECS);
    set_user_quota(broker, user, "consumer_byte_rate", rate).await;
}

/// What one fetch answered.
#[derive(Debug, PartialEq, Eq)]
struct FetchOutcome {
    throttled: bool,
    rows: usize,
    acquired: Vec<OffsetRun>,
}

impl FetchOutcome {
    fn of(response: &ShareFetchResponse) -> Self {
        let rows = share_fetch_partition_rows(response.clone());
        Self {
            throttled: response.throttle_time_ms > 0,
            rows: rows.len(),
            acquired: rows.first().map_or_default(acquired),
        }
    }

    fn sent(first_offset: i64, last_offset: i64) -> Self {
        Self {
            throttled: false,
            rows: 1,
            acquired: vec![OffsetRun::new(first_offset, last_offset)],
        }
    }

    /// An incremental answer that has nothing new for the partition.
    fn nothing_new() -> Self {
        Self {
            throttled: false,
            rows: 0,
            acquired: Vec::new(),
        }
    }

    fn throttled() -> Self {
        Self {
            throttled: true,
            rows: 0,
            acquired: Vec::new(),
        }
    }
}

/// Three fetches of one batch each, on a topic of two batches.
///
/// A byte-rate bucket that holds a batch and a half. The first fetch fits. The
/// second takes the bucket past it, so it is throttled and answers no rows,
/// and its batch stays acquired by the member. Its bytes come off the quota
/// again, so the third fetch, which acquires nothing, is not throttled.
/// Charged without the refund, it would be. A starved request quota throttles
/// every fetch.
#[tokio::test]
async fn share_fetch_is_charged_to_the_member_quotas() {
    let HandlerStartedBroker { broker, dir: _dir } = start().await;

    let cases = [
        ("no-quota", MemberQuota::None, [
            FetchOutcome::sent(0, 1),
            FetchOutcome::sent(2, 3),
            FetchOutcome::nothing_new(),
        ]),
        ("byte-rate", MemberQuota::BatchesPerWindow(1.5), [
            FetchOutcome::sent(0, 1),
            FetchOutcome::throttled(),
            FetchOutcome::nothing_new(),
        ]),
        (
            "request-percentage",
            MemberQuota::RequestPercentage(STARVED_REQUEST_PERCENTAGE),
            [
                FetchOutcome::throttled(),
                FetchOutcome::throttled(),
                FetchOutcome::throttled(),
            ],
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, quota, want) in cases {
        let topic = format!("quota-{name}");
        let group = format!("group-{name}");
        let user = format!("user-{name}");
        let topic_id = open_session(&broker, &topic, &group, &user).await;
        match quota {
            MemberQuota::None => {}
            MemberQuota::BatchesPerWindow(batches) => {
                set_batches_per_window(&broker, &topic, &user, batches).await;
            }
            MemberQuota::RequestPercentage(percentage) => {
                set_user_quota(&broker, &user, "request_percentage", percentage).await;
            }
        }

        let mut outcomes = Vec::new();
        for epoch in 1..=3 {
            let response = fetch()
                .broker(&broker)
                .group(&group)
                .user(&user)
                .topic_id(topic_id)
                .epoch(epoch)
                .call()
                .await;
            outcomes.push(FetchOutcome::of(&response));
        }
        actual.push((name, quota, outcomes));
        expected.push((name, quota, Vec::from(want)));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka caps a share fetch at `maxQuotaWindowBytes`, the bytes the member's
/// byte rate allows in one quota window, which is half of what its bucket
/// holds. A member whose bucket holds a batch and a half asks for the whole
/// topic. Uncapped, it would acquire both batches, overflow the bucket, and
/// be throttled with its records dropped on every attempt. Capped, it gets the
/// first batch, unthrottled.
#[tokio::test]
async fn a_share_fetch_acquires_no_more_than_the_quota_window_allows() {
    let HandlerStartedBroker { broker, dir: _dir } = start().await;
    let (topic, group, user) = ("quota-window", "group-window", "user-window");
    let topic_id = open_session(&broker, topic, group, user).await;
    set_batches_per_window(&broker, topic, user, 1.5).await;

    let response = fetch()
        .broker(&broker)
        .group(group)
        .user(user)
        .topic_id(topic_id)
        .epoch(1)
        .max_records(500)
        .call()
        .await;

    assert!(FetchOutcome::of(&response) == FetchOutcome::sent(0, 1));
    broker.shutdown().await;
}

/// A throttled fetch drops its records, but not its acknowledgements: Kafka
/// applies them, and answers each one in a row of its own beside the
/// throttle. The records the fetch acquired stay acquired.
#[tokio::test]
async fn a_throttled_share_fetch_still_answers_its_acknowledgements() {
    let HandlerStartedBroker { broker, dir: _dir } = start().await;
    let (topic, group, user) = (
        "quota-acknowledged",
        "group-acknowledged",
        "user-acknowledged",
    );
    let topic_id = open_session(&broker, topic, group, user).await;
    let first = fetch()
        .broker(&broker)
        .group(group)
        .user(user)
        .topic_id(topic_id)
        .epoch(1)
        .call()
        .await;
    assert!(FetchOutcome::of(&first) == FetchOutcome::sent(0, 1));
    set_user_quota(
        &broker,
        user,
        "request_percentage",
        STARVED_REQUEST_PERCENTAGE,
    )
    .await;

    let throttled = fetch()
        .broker(&broker)
        .group(group)
        .user(user)
        .topic_id(topic_id)
        .epoch(2)
        .acknowledgement_batches(vec![AcknowledgementBatch {
            first_offset: 0,
            last_offset: 1,
            acknowledge_types: vec![ACCEPT],
            ..Default::default()
        }])
        .call()
        .await;

    assert!(throttled.throttle_time_ms > 0, "{throttled:?}");
    assert!(
        throttled
            == ShareFetchResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                acquisition_lock_timeout_ms: first.acquisition_lock_timeout_ms,
                responses: vec![ShareFetchableTopicResponse {
                    topic_id,
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        acknowledge_error_code: codes::NONE,
                        records: Some(RecordsPayload::Legacy(Bytes::new())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
    // The accepted records leave the in-flight state as the share-partition
    // start offset passes them, and the throttled fetch's batch stays
    // acquired.
    assert!(share_record_states(&broker, group, topic_id).await == vec![Acquired, Acquired]);
    broker.shutdown().await;
}
