//! The two read-only listing APIs of this suite: `ListConfigResources`
//! (`api_key` 74, KIP-1142), whose default set covers every topic and broker, and
//! `ListGroups` (`api_key` 16). Both check the dispatch glue, the ACL gate's
//! allow path, and the response encoding, so they share one file.

use {
    crate::{
        RESOURCE_TYPE_TOPIC,
        admin_harness::{AdminCluster, admin, create_topic_helper},
    },
    assert2::assert,
    krabka_protocol::owned::{
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::ListConfigResourcesResponse,
        list_groups_request::ListGroupsRequest, list_groups_response::ListGroupsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct AdminListings {
    resources: Option<ListConfigResourcesResponse>,
    groups: Option<ListGroupsResponse>,
}

#[fixture]
fn listing() -> AdminListings {
    // Start this scenario with fresh inputs and observations.

    AdminListings {
        resources: None,
        groups: None,
    }
}

#[given("topics t-lcr-a and t-lcr-b each have one partition")]
async fn listing_topics(admin: &AdminCluster) {
    create_topic_helper(admin.client(), "t-lcr-a", 1).await;
    create_topic_helper(admin.client(), "t-lcr-b", 1).await;
}

#[given("the broker already knows consumer group test-group-listed")]
fn listing_group(admin: &AdminCluster) {
    admin.broker().group_create_for_test("test-group-listed");
}

#[when("the admin lists configuration resources without a type filter")]
async fn list_resources(admin: &AdminCluster, listing: &mut AdminListings) {
    listing.resources = Some(
        admin
            .client()
            .send(ListConfigResourcesRequest::default())
            .await
            .expect("list_config_resources"),
    );
}

#[when("the admin lists consumer groups")]
async fn list_groups(admin: &AdminCluster, listing: &mut AdminListings) {
    listing.groups = Some(
        admin
            .client()
            .send(ListGroupsRequest::default())
            .await
            .expect("list_groups"),
    );
}

#[then("the resource list succeeds and includes both topics and exactly broker 1")]
fn resource_listing(listing: &AdminListings) {
    let response = listing
        .resources
        .as_ref()
        .expect("configuration resources were listed");
    assert!(response.error_code == 0, "list_config_resources error_code");
    let topics: Vec<&str> = response
        .config_resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|resource| resource.resource_name.as_str())
        .collect();
    assert!(
        topics.contains(&"t-lcr-a") && topics.contains(&"t-lcr-b"),
        "expected both seeded topics in response, got {topics:?}"
    );
    let brokers: Vec<&str> = response
        .config_resources
        .iter()
        .filter(|resource| resource.resource_type == 4)
        .map(|resource| resource.resource_name.as_str())
        .collect();
    assert!(
        brokers == vec!["1"],
        "expected exactly broker '1', got {brokers:?}"
    );
}

#[then("the group list succeeds and contains test-group-listed")]
fn group_listing(listing: &AdminListings) {
    let response = listing
        .groups
        .as_ref()
        .expect("consumer groups were listed");
    assert!(response.error_code == 0, "list_groups error_code");
    let ids: Vec<&str> = response
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect();
    assert!(
        ids.contains(&"test-group-listed"),
        "expected `test-group-listed` in group list, got {ids:?}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_listings.feature",
    name = "list config resources default set includes topics and brokers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_config_resources_default_set_includes_topics_and_brokers(
    admin: AdminCluster,
    listing: AdminListings,
) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_listings.feature",
    name = "list groups includes freshly created group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_freshly_created_group(admin: AdminCluster, listing: AdminListings) {
    admin.shutdown().await;
}
