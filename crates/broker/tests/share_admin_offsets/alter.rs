//! Tests for `AlterShareGroupOffsets`, `api_key` 91.
//!
//! Alter resets the share-partition start offset of an empty group, and the
//! broker fences it with `NON_EMPTY_GROUP` while the group still has a live
//! member.

use {
    crate::{
        describe::describe_until,
        harness::{NON_EMPTY_GROUP, NONE, fetch_until_acquired, join, wait_for_share_init},
        support::share::{MemberSession, ShareSessionSetup},
    },
    assert2::assert,
    krabka_protocol::owned::alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
        AlterShareGroupOffsetsRequestTopic,
    },
    std::time::Duration,
    tokio::time,
};

fn alter_request(start_offset: i64) -> AlterShareGroupOffsetsRequest {
    AlterShareGroupOffsetsRequest {
        group_id: "g1".into(),
        topics: vec![AlterShareGroupOffsetsRequestTopic {
            topic_name: "t".into(),
            partitions: vec![AlterShareGroupOffsetsRequestPartition {
                partition_index: 0,
                start_offset,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

use {
    crate::harness::AdminOffsetSession,
    krabka_protocol::owned::{
        alter_share_group_offsets_response::AlterShareGroupOffsetsResponse,
        share_fetch_response::PartitionData,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ShareOffsetAlteration {
    session: Option<AdminOffsetSession>,
    response: Option<AlterShareGroupOffsetsResponse>,
    described: Option<i64>,
    fetched: Option<PartitionData>,
}

#[fixture]
fn share_offset_alteration() -> ShareOffsetAlteration {
    // Keep the mutation response distinct from described and acquired offsets.
    ShareOffsetAlteration::default()
}

#[given("an empty share group g1 with {records} records on t partition 0")]
async fn empty_alter_group(share_offset_alteration: &mut ShareOffsetAlteration, records: i64) {
    share_offset_alteration.session = Some(AdminOffsetSession::seeded(records).await);
}

#[given("a live share member has joined g1 and initialized its state")]
async fn occupied_alter_group(share_offset_alteration: &mut ShareOffsetAlteration) {
    share_offset_alteration
        .session
        .as_mut()
        .expect("seeded group")
        .member = Some(
        join(&share_offset_alteration.session().client, "g1", "t")
            .await
            .member,
    );
    wait_for_share_init(
        share_offset_alteration.session().broker(),
        "g1",
        share_offset_alteration.session().topic,
        0,
    )
    .await;
}

#[when("an administrator resets empty g1 to offset 5, retrying while its persister settles")]
async fn reset_empty_share_group(share_offset_alteration: &mut ShareOffsetAlteration) {
    for _ in 0..40 {
        let response = share_offset_alteration
            .session()
            .client
            .send(alter_request(5))
            .await
            .expect("AlterShareGroupOffsets");
        let accepted =
            response.error_code == NONE && response.responses[0].partitions[0].error_code == NONE;
        share_offset_alteration.response = Some(response);
        if accepted {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("both the group and partition offset-reset responses succeed")]
fn share_offset_reset(share_offset_alteration: &ShareOffsetAlteration) {
    let response = share_offset_alteration
        .response
        .as_ref()
        .expect("offset reset attempted");
    assert!(
        response.error_code == NONE && response.responses[0].partitions[0].error_code == NONE,
        "AlterShareGroupOffsets never succeeded: {response:?}"
    );
}

#[when("DescribeShareGroupOffsets observes the reset start offset")]
async fn describe_reset_share_group(share_offset_alteration: &mut ShareOffsetAlteration) {
    share_offset_alteration.described = Some(
        describe_until(
            &share_offset_alteration.session().client,
            "g1",
            "t",
            vec![0],
            5,
        )
        .await
        .topics[0]
            .partitions[0]
            .start_offset,
    );
}

#[then("the described share start offset is 5")]
fn reset_share_start(share_offset_alteration: &ShareOffsetAlteration) {
    assert!(share_offset_alteration.described == Some(5));
}

#[when("a member joins reset g1 and acquires its first available records")]
async fn acquire_reset_share_group(share_offset_alteration: &mut ShareOffsetAlteration) {
    let session = share_offset_alteration
        .session
        .as_mut()
        .expect("seeded group");
    let MemberSession { member, .. } = join(&session.client, "g1", "t").await;
    share_offset_alteration.fetched = Some(
        fetch_until_acquired(
            &session.client,
            ShareSessionSetup::opening(&member, session.topic),
        )
        .await,
    );
    session.member = Some(member);
}

#[then("the member first acquires offset 5")]
fn reset_acquisition(share_offset_alteration: &ShareOffsetAlteration) {
    let response = share_offset_alteration
        .fetched
        .as_ref()
        .expect("share records acquired");
    assert!(
        response.acquired_records[0].first_offset == 5,
        "{:?}",
        response.acquired_records
    );
}

#[when("an administrator tries to reset occupied g1 to offset 1")]
async fn reset_occupied_share_group(share_offset_alteration: &mut ShareOffsetAlteration) {
    share_offset_alteration.response = Some(
        share_offset_alteration
            .session()
            .client
            .send(alter_request(1))
            .await
            .expect("AlterShareGroupOffsets"),
    );
}

#[then("the offset reset is refused at group level with NON_EMPTY_GROUP")]
fn occupied_reset_refused(share_offset_alteration: &ShareOffsetAlteration) {
    let response = share_offset_alteration
        .response
        .as_ref()
        .expect("offset reset attempted");
    assert!(response.error_code == NON_EMPTY_GROUP, "{response:?}");
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/alter.feature",
    name = "alter resets empty group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_resets_empty_group(share_offset_alteration: ShareOffsetAlteration) {
    share_offset_alteration
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_offset_alteration);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/alter.feature",
    name = "alter non empty group fenced"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_non_empty_group_fenced(share_offset_alteration: ShareOffsetAlteration) {
    share_offset_alteration
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_offset_alteration);
}

impl ShareOffsetAlteration {
    fn session(&self) -> &AdminOffsetSession {
        self.session.as_ref().expect("live scenario session")
    }
}
