//! Tests for `DeleteShareGroupOffsets`, `api_key` 92.
//!
//! Delete removes the durable share-state of a topic in an empty group, so a
//! later Describe reads the partition as missing. It also rewrites the v14
//! state-partition-metadata record, so the topic stays out of the initialized
//! set of the group across a restart.

use {
    crate::{
        describe::{describe_all_offsets, describe_until, initialized_partitions},
        harness::{NONE, fetch_until_acquired, leave},
        support::share::{ShareSessionSetup, rejoin_group},
    },
    assert2::{assert, check},
    krabka_protocol::owned::delete_share_group_offsets_request::{
        DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
    },
    std::time::{Duration, Instant},
    tokio::time,
};

fn delete_topic_request() -> DeleteShareGroupOffsetsRequest {
    DeleteShareGroupOffsetsRequest {
        group_id: "g1".into(),
        topics: vec![DeleteShareGroupOffsetsRequestTopic {
            topic_name: "t".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

use {
    crate::harness::AdminOffsetSession,
    krabka_protocol::owned::{
        delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse,
        describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ShareOffsetDeletion {
    session: Option<AdminOffsetSession>,
    response: Option<DeleteShareGroupOffsetsResponse>,
    described: Option<DescribeShareGroupOffsetsResponseGroup>,
    enumerated: Option<Vec<i32>>,
}

#[fixture]
fn share_offset_deletion() -> ShareOffsetDeletion {
    // Capture deletion and metadata enumeration independently across the restart.
    ShareOffsetDeletion::default()
}

#[given("share group g1 has acquired its three initialized records on t")]
async fn initialized_deletion_group(share_offset_deletion: &mut ShareOffsetDeletion) {
    share_offset_deletion.session = Some(AdminOffsetSession::initialized(3).await);
    fetch_until_acquired(
        &share_offset_deletion.session().client,
        ShareSessionSetup::opening(
            share_offset_deletion
                .session()
                .member
                .as_ref()
                .expect("joined member"),
            share_offset_deletion.session().topic,
        ),
    )
    .await;
}

#[when("the deletion-test member leaves g1")]
async fn empty_deletion_group(share_offset_deletion: &ShareOffsetDeletion) {
    leave(
        &share_offset_deletion.session().client,
        "g1",
        share_offset_deletion
            .session()
            .member
            .as_ref()
            .expect("joined member"),
    )
    .await;
}

#[when("an administrator deletes t's offsets from g1")]
async fn delete_share_offsets(share_offset_deletion: &mut ShareOffsetDeletion) {
    share_offset_deletion.response = Some(
        share_offset_deletion
            .session()
            .client
            .send(delete_topic_request())
            .await
            .expect("DeleteShareGroupOffsets"),
    );
}

#[then("offset deletion succeeds for the group and topic t")]
fn share_offsets_deleted(share_offset_deletion: &ShareOffsetDeletion) {
    let response = share_offset_deletion
        .response
        .as_ref()
        .expect("DeleteShareGroupOffsets answered");
    check!(response.error_code == NONE);
    check!(response.responses[0].topic_name == "t");
    check!(response.responses[0].error_code == NONE);
}

#[when("the deleted t partition is described until it reports a missing start")]
async fn describe_deleted_share_offsets(share_offset_deletion: &mut ShareOffsetDeletion) {
    share_offset_deletion.described = Some(
        describe_until(
            &share_offset_deletion.session().client,
            "g1",
            "t",
            vec![0],
            -1,
        )
        .await,
    );
}

#[then("the deleted partition's start is minus one without a partition error")]
fn missing_deleted_share_offsets(share_offset_deletion: &ShareOffsetDeletion) {
    let partition = &share_offset_deletion
        .described
        .as_ref()
        .expect("partition described")
        .topics[0]
        .partitions[0];
    assert!(partition.start_offset == -1);
    assert!(partition.error_code == NONE);
}

#[when("all initialized share offsets of g1 are enumerated")]
async fn enumerate_deleted_share_offsets(share_offset_deletion: &mut ShareOffsetDeletion) {
    let described = describe_all_offsets(&share_offset_deletion.session().client, "g1").await;
    share_offset_deletion.enumerated = Some(initialized_partitions(&described, "t"));
}

#[then("the initialized offset metadata enumerates exactly t partition zero")]
fn initialized_deletion_metadata(share_offset_deletion: &ShareOffsetDeletion) {
    assert!(
        share_offset_deletion
            .enumerated
            .as_ref()
            .expect("initialized offsets enumerated")
            == &vec![0]
    );
}

#[when("offset metadata is polled for up to four seconds until deleted t disappears")]
async fn await_deleted_share_metadata(share_offset_deletion: &mut ShareOffsetDeletion) {
    for _ in 0..40 {
        enumerate_deleted_share_offsets(share_offset_deletion).await;
        if share_offset_deletion
            .enumerated
            .as_ref()
            .expect("enumeration")
            .is_empty()
        {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("the initialized offset metadata contains no t partitions")]
fn absent_deletion_metadata(share_offset_deletion: &ShareOffsetDeletion) {
    assert!(
        share_offset_deletion
            .enumerated
            .as_ref()
            .expect("initialized offsets enumerated")
            .is_empty()
    );
}

#[when("the metadata rewrite is flushed for 300 milliseconds and the share broker restarts")]
async fn restart_deleted_share_metadata(share_offset_deletion: &mut ShareOffsetDeletion) {
    let session = share_offset_deletion.session.as_mut().expect("share group");
    time::sleep(Duration::from_millis(300)).await;
    session
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = rejoin_group(session.directory.path().to_path_buf(), "g1").await;
    session.broker = Some(broker);
    session.client = client;
}

#[then("deleted t remains absent from initialized offsets throughout eight seconds of recovery")]
async fn durable_deleted_share_metadata(share_offset_deletion: &ShareOffsetDeletion) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let described = describe_all_offsets(&share_offset_deletion.session().client, "g1").await;
        let partitions = initialized_partitions(&described, "t");
        assert!(
            partitions.is_empty(),
            "deleted topic reinitialized after restart: {partitions:?}"
        );
        if Instant::now() >= deadline {
            break;
        }
        time::sleep(Duration::from_millis(200)).await;
    }
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/delete.feature",
    name = "delete removes topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_topic(share_offset_deletion: ShareOffsetDeletion) {
    share_offset_deletion
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_offset_deletion);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/delete.feature",
    name = "delete rewrites metadata topic absent after restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_rewrites_metadata_topic_absent_after_restart(
    share_offset_deletion: ShareOffsetDeletion,
) {
    share_offset_deletion
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_offset_deletion);
}

impl ShareOffsetDeletion {
    fn session(&self) -> &AdminOffsetSession {
        self.session.as_ref().expect("live scenario session")
    }
}
