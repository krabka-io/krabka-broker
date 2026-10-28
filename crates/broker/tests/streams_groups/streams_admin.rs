//! The admin-visibility scenarios for a live streams group: what
//! `StreamsGroupDescribe` reports about it, and how `ListGroups` tags and
//! filters it.
//!
//! These are the two requests the JVM `kafka-streams-groups.sh` tool issues, so
//! they are grouped here rather than with the membership scenarios that assert
//! on heartbeat responses.

use {
    crate::streams_harness::{StreamsGroupScenario, streams_group},
    assert2::{assert, check},
    krabka_protocol::owned::{
        list_groups_request::ListGroupsRequest, list_groups_response::ListGroupsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{scenario, then, when},
};

#[derive(Default)]
struct StreamsListing {
    response: Option<ListGroupsResponse>,
}

#[fixture]
fn streams_listing() -> StreamsListing {
    // Keep the actual filtered list separate from the established streams group.
    StreamsListing::default()
}

#[then("the streams description identifies the live group, its member, and a nonempty state")]
fn visible_streams_group(streams_group: &StreamsGroupScenario) {
    let described = streams_group
        .described
        .as_ref()
        .expect("streams group described");
    assert!(described.groups.len() == 1);
    let group = &described.groups[0];
    check!(group.error_code == 0);
    check!(group.group_id == streams_group.group);
    check!(!group.members.is_empty());
    check!(
        group
            .members
            .iter()
            .any(|member| member.member_id == streams_group.member)
    );
    check!(!group.group_state.is_empty());
}

#[when("the streams administrator lists groups filtered by {kind}")]
async fn list_streams_group(
    streams_group: &StreamsGroupScenario,
    streams_listing: &mut StreamsListing,
    kind: String,
) {
    streams_listing.response = Some(
        streams_group
            .client()
            .send(ListGroupsRequest {
                types_filter: vec![kind],
                ..Default::default()
            })
            .await
            .expect("ListGroups"),
    );
}

#[then("the filtered list contains the group tagged as streams")]
fn listed_streams_group(streams_group: &StreamsGroupScenario, streams_listing: &StreamsListing) {
    let response = streams_listing
        .response
        .as_ref()
        .expect("ListGroups answered");
    assert!(response.error_code == 0, "{response:?}");
    let group = response
        .groups
        .iter()
        .find(|group| group.group_id == streams_group.group)
        .expect("streams group listed");
    assert!(group.group_type == "streams");
}

#[then("the filtered list excludes the streams group")]
fn excluded_streams_group(streams_group: &StreamsGroupScenario, streams_listing: &StreamsListing) {
    assert!(
        !streams_listing
            .response
            .as_ref()
            .expect("ListGroups answered")
            .groups
            .iter()
            .any(|group| group.group_id == streams_group.group)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_admin.feature",
    name = "describe returns the group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_returns_the_group(streams_group: StreamsGroupScenario) {
    streams_group.shutdown().await;
    drop(streams_group);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_admin.feature",
    name = "list groups surfaces streams group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_surfaces_streams_group(
    streams_group: StreamsGroupScenario,
    streams_listing: StreamsListing,
) {
    let _ = streams_listing;
    streams_group.shutdown().await;
    drop(streams_group);
}
