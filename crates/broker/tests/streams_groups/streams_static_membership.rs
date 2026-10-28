//! Static membership (`instance_id`) in a streams group, as Kafka 4.3.1 serves
//! it: not at all.
//!
//! Kafka 4.3.1's `GroupCoordinatorService.streamsGroupHeartbeat` runs
//! `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures` before it
//! schedules the write, and that check refuses any request with an instance id
//! with `INVALID_REQUEST` and "Static membership is not yet supported.". The
//! coordinator never sees the request, so it writes no record, creates no
//! group and changes no member. Its own static paths (`streamsGroupLeave` and
//! `streamsGroupHeartbeat`) throw `UnsupportedOperationException` and are
//! unreachable. The broker's default configuration serves 4.3.1.

use {
    crate::streams_harness::{
        StreamsGroupScenario, describe, first_join, follow_up, streams_group, topology,
    },
    assert2::check,
    krabka_protocol::owned::{
        streams_group_describe_response::StreamsGroupDescribeResponse,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
};

const INVALID_REQUEST: i16 = 42;
const GROUP_ID_NOT_FOUND: i16 = 69;

#[derive(Default)]
struct StaticStreamsRequests {
    before: Option<StreamsGroupDescribeResponse>,
    answers: Vec<StreamsGroupHeartbeatResponse>,
    after: Option<StreamsGroupDescribeResponse>,
    absent: Option<StreamsGroupDescribeResponse>,
}

#[fixture]
fn static_streams_requests() -> StaticStreamsRequests {
    // Compare full group descriptions before and after the refused static requests.
    StaticStreamsRequests::default()
}

#[derive(DataTableRow)]
struct StaticRequest {
    kind: String,
}

#[given("the existing streams group's complete description is recorded")]
async fn record_static_streams_group(
    streams_group: &StreamsGroupScenario,
    static_streams_requests: &mut StaticStreamsRequests,
) {
    static_streams_requests.before =
        Some(describe(streams_group.client(), &streams_group.group).await);
}

#[when("the broker receives these instance-1 streams requests")]
async fn request_static_streams_membership(
    streams_group: &StreamsGroupScenario,
    static_streams_requests: &mut StaticStreamsRequests,
    #[datatable] rows: Rows<StaticRequest>,
) {
    let epoch = streams_group
        .heartbeat
        .as_ref()
        .expect("live join response")
        .member_epoch;
    for row in rows {
        let join = || first_join(&streams_group.group, topology(&streams_group.topic, vec![]));
        let request = match row.kind.as_str() {
            "first join" => join(),
            "known member rejoin" => StreamsGroupHeartbeatRequest {
                member_id: streams_group.member.clone(),
                ..join()
            },
            "known member heartbeat" => follow_up()
                .group(&streams_group.group)
                .member_id(&streams_group.member)
                .epoch(epoch)
                .call(),
            "unknown member heartbeat" => follow_up()
                .group(&streams_group.group)
                .member_id("another-member")
                .epoch(epoch)
                .call(),
            "temporary leave" => follow_up()
                .group(&streams_group.group)
                .member_id(&streams_group.member)
                .epoch(-2)
                .call(),
            "permanent leave" => follow_up()
                .group(&streams_group.group)
                .member_id(&streams_group.member)
                .epoch(-1)
                .call(),
            "absent group first join" => {
                first_join("static-absent", topology(&streams_group.topic, vec![]))
            }
            _ => panic!("unknown static request {}", row.kind),
        };
        static_streams_requests.answers.push(
            streams_group
                .client()
                .send(StreamsGroupHeartbeatRequest {
                    instance_id: Some("instance-1".into()),
                    ..request
                })
                .await
                .expect("StreamsGroupHeartbeat"),
        );
    }
}

#[then("all seven static requests return the complete unsupported-static-membership refusal")]
fn unsupported_static_streams_membership(static_streams_requests: &StaticStreamsRequests) {
    check!(static_streams_requests.answers.len() == 7);
    let refusal = StreamsGroupHeartbeatResponse {
        error_code: INVALID_REQUEST,
        error_message: Some("Static membership is not yet supported.".into()),
        ..Default::default()
    };
    for answer in &static_streams_requests.answers {
        check!(answer == &refusal);
    }
}

#[when("the existing and absent streams groups are described after the refusals")]
async fn inspect_static_streams_membership(
    streams_group: &StreamsGroupScenario,
    static_streams_requests: &mut StaticStreamsRequests,
) {
    static_streams_requests.after =
        Some(describe(streams_group.client(), &streams_group.group).await);
    static_streams_requests.absent = Some(describe(streams_group.client(), "static-absent").await);
}

#[then(
    "the complete existing streams description is unchanged and static-absent was never created"
)]
fn unchanged_static_streams_membership(static_streams_requests: &StaticStreamsRequests) {
    check!(
        static_streams_requests
            .after
            .as_ref()
            .expect("existing group described")
            == static_streams_requests
                .before
                .as_ref()
                .expect("original group recorded")
    );
    check!(
        static_streams_requests
            .absent
            .as_ref()
            .expect("absent group described")
            .groups
            .iter()
            .map(|group| group.error_code)
            .collect::<Vec<_>>()
            == vec![GROUP_ID_NOT_FOUND]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_static_membership.feature",
    name = "every static member request is refused and changes nothing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_static_member_request_is_refused_and_changes_nothing(
    streams_group: StreamsGroupScenario,
    static_streams_requests: StaticStreamsRequests,
) {
    let _ = static_streams_requests;
    streams_group.shutdown().await;
    drop(streams_group);
}
