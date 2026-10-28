//! Coverage for the `ListGroups` view of a share group, which is the request
//! `kafka-share-groups.sh --list` issues.
//!
//! The scenario lives in its own module because it exercises `ListGroups`
//! rather than the share-group RPCs, and asserts on the `group_type` tagging
//! and on the `types_filter` behaviour.

use {
    crate::{
        share_group_harness::{boot, connect, create_topic, join_share_group},
        support::client::list_groups_by_type,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        list_groups_request::ListGroupsRequest, list_groups_response::ListGroupsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct ShareGroupListing {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Arc<Client>>,
    listed: Option<ListGroupsResponse>,
}

#[fixture]
fn share_group_listing() -> ShareGroupListing {
    // Each filter is exercised against a real coordinator holding a live share group.
    ShareGroupListing::default()
}

#[given("share group g7 has joined two-partition topic t7")]
async fn listed_share_group(share_group_listing: &mut ShareGroupListing) {
    let (broker, bootstrap, directory) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t7", 2).await;
    join_share_group(&client, "g7", &uuid::Uuid::new_v4().to_string(), "t7").await;
    share_group_listing.broker = Some(broker);
    share_group_listing.directory = Some(directory);
    share_group_listing.client = Some(client);
}

#[when("ListGroups uses the {filter} group-type filter")]
async fn list_share_group(share_group_listing: &mut ShareGroupListing, filter: String) {
    let client = share_group_listing.client.as_ref().expect("group client");
    share_group_listing.listed = Some(if filter == "all" {
        client
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups all")
    } else {
        list_groups_by_type(client, &filter).await
    });
}

#[then("g7 is present with group type share: {present}")]
fn share_group_type(share_group_listing: &ShareGroupListing, present: bool) {
    let response = share_group_listing
        .listed
        .as_ref()
        .expect("ListGroups answered");
    let row = response.groups.iter().find(|group| group.group_id == "g7");
    assert!(
        row.is_some() == present,
        "unexpected filtered groups: {:?}",
        response.groups
    );
    if present {
        assert!(row.expect("g7 present").group_type == "share");
    }
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_listing.feature",
    name = "list groups includes share group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_share_group(
    share_group_listing: ShareGroupListing,
    filter: String,
    present: bool,
) {
    let _ = (filter, present);
    share_group_listing
        .broker
        .take()
        .expect("group broker")
        .shutdown()
        .await;
    drop(share_group_listing);
}

use std::sync::Arc;
