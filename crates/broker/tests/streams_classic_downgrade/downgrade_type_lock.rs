//! The type lock that guards the downgrade: a streams group with a live member
//! refuses a classic `JoinGroup`, static or dynamic, and the classic admin path
//! sees the converted group as `streams` and refuses to delete it while a
//! member is live.

use {
    crate::{
        ERR_INCONSISTENT_GROUP_PROTOCOL, ERR_NON_EMPTY_GROUP,
        downgrade_classic_join::{classic_join_sync, join_request},
        downgrade_harness::{DowngradeScenario, downgrade_case},
    },
    assert2::{assert, check},
    krabka_broker::coordinator::unified::GroupType,
    krabka_protocol::owned::{
        delete_groups_request::DeleteGroupsRequest,
        join_group_request::JoinGroupRequest,
        join_group_response::JoinGroupResponse,
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        list_groups_request::ListGroupsRequest,
        list_groups_response::ListGroupsResponse,
    },
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, scenario, then, when},
    std::time::Duration,
    tokio::time,
};

#[derive(Default)]
struct DowngradeTypeLock {
    joins: Vec<JoinGroupResponse>,
    listed: Option<ListGroupsResponse>,
    deleted: Option<i16>,
}

#[fixture]
fn downgrade_type_lock() -> DowngradeTypeLock {
    // Preserve complete refused joins independently of classic admin observations.
    DowngradeTypeLock::default()
}

#[derive(DataTableRow)]
struct ClassicJoinInput {
    member: String,
    instance: String,
}

#[then("the downgrade group retains one live Streams member")]
async fn locked_streams_membership(downgrade_case: &DowngradeScenario) {
    assert!(
        downgrade_case
            .broker()
            .group_type_for_test(&downgrade_case.group)
            == Some(GroupType::Streams)
    );
    downgrade_case
        .broker()
        .wait_until_streams_group_member_count(&downgrade_case.group, 1)
        .await;
}

#[when("these classic JoinGroup requests target the live streams group")]
async fn request_locked_downgrade(
    downgrade_case: &DowngradeScenario,
    downgrade_type_lock: &mut DowngradeTypeLock,
    #[datatable] rows: Rows<ClassicJoinInput>,
) {
    for row in rows {
        let request = JoinGroupRequest {
            group_instance_id: (row.instance != "absent").then_some(row.instance),
            ..join_request(
                &downgrade_case.group,
                if row.member == "empty" {
                    ""
                } else {
                    &row.member
                },
            )
        };
        downgrade_type_lock.joins.push(
            time::timeout(
                Duration::from_secs(5),
                downgrade_case.classic().send(request),
            )
            .await
            .expect("JoinGroup timeout")
            .expect("JoinGroup"),
        );
    }
}

#[then("all four classic joins return the complete INCONSISTENT_GROUP_PROTOCOL refusal")]
fn refused_streams_downgrade(downgrade_type_lock: &DowngradeTypeLock) {
    assert!(downgrade_type_lock.joins.len() == 4);
    let refused = JoinGroupResponse {
        error_code: ERR_INCONSISTENT_GROUP_PROTOCOL,
        member_id: String::new(),
        protocol_name: None,
        ..Default::default()
    };
    for response in &downgrade_type_lock.joins {
        check!(response == &refused);
    }
}

#[when("a classic group is formed and its member leaves before streams conversion")]
async fn classic_group_for_locked_upgrade(downgrade_case: &DowngradeScenario) {
    let (member, _) = classic_join_sync(downgrade_case.classic(), &downgrade_case.group).await;
    downgrade_case
        .classic()
        .send(LeaveGroupRequest {
            group_id: downgrade_case.group.clone(),
            member_id: member.clone(),
            members: vec![MemberIdentity {
                member_id: member,
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    downgrade_case
        .broker()
        .wait_until_group_empty(&downgrade_case.group)
        .await;
}

#[when("the classic admin client lists all groups after streams conversion")]
async fn list_locked_streams_group(
    downgrade_case: &DowngradeScenario,
    downgrade_type_lock: &mut DowngradeTypeLock,
) {
    downgrade_type_lock.listed = Some(
        downgrade_case
            .classic()
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups"),
    );
}

#[then("the converted group is listed exactly once with streams type")]
fn visible_locked_streams_group(
    downgrade_case: &DowngradeScenario,
    downgrade_type_lock: &DowngradeTypeLock,
) {
    let rows: Vec<_> = downgrade_type_lock
        .listed
        .as_ref()
        .expect("groups listed")
        .groups
        .iter()
        .filter(|group| group.group_id == downgrade_case.group)
        .collect();
    assert!(rows.len() == 1);
    assert!(rows[0].group_type.eq_ignore_ascii_case("streams"));
}

#[when("the classic admin client attempts to delete the live converted streams group")]
async fn delete_locked_streams_group(
    downgrade_case: &DowngradeScenario,
    downgrade_type_lock: &mut DowngradeTypeLock,
) {
    let response = downgrade_case
        .classic()
        .send(DeleteGroupsRequest {
            groups_names: vec![downgrade_case.group.clone()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    downgrade_type_lock.deleted = Some(response.results[0].error_code);
}

#[then("the live converted group refuses deletion with NON_EMPTY_GROUP and stays Streams-typed")]
fn retained_locked_streams_group(
    downgrade_case: &DowngradeScenario,
    downgrade_type_lock: &DowngradeTypeLock,
) {
    assert!(downgrade_type_lock.deleted == Some(ERR_NON_EMPTY_GROUP));
    assert!(
        downgrade_case
            .broker()
            .group_type_for_test(&downgrade_case.group)
            == Some(GroupType::Streams)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_downgrade/downgrade_type_lock.feature",
    name = "streams group with live member rejects classic join"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_group_with_live_member_rejects_classic_join(
    downgrade_case: DowngradeScenario,
    downgrade_type_lock: DowngradeTypeLock,
) {
    let _ = downgrade_type_lock;
    downgrade_case.shutdown().await;
    drop(downgrade_case);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_downgrade/downgrade_type_lock.feature",
    name = "converted group admin views respect type lock"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn converted_group_admin_views_respect_type_lock(
    downgrade_case: DowngradeScenario,
    downgrade_type_lock: DowngradeTypeLock,
) {
    let _ = downgrade_type_lock;
    downgrade_case.shutdown().await;
    drop(downgrade_case);
}
