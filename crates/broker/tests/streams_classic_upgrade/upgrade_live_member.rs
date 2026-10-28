//! The rejection scenario: a classic group that still has a live member, static
//! or dynamic, must refuse the `StreamsGroupHeartbeat` with
//! `GROUP_ID_NOT_FOUND` (69) and stay Classic-typed.
//!
//! The scenario parks a `JoinGroup` on its own connection to hold the member
//! live, which is why it does not reuse the full `classic_join_sync` driver and
//! lives apart from the conversion scenario.

use {
    crate::{
        support::client::connect_client,
        upgrade_classic::join_request,
        upgrade_harness::{
            ERR_GROUP_ID_NOT_FOUND, ERR_MEMBER_ID_REQUIRED, UpgradeScenario, connect, upgrade_case,
        },
        upgrade_streams::{first_join, topology},
    },
    assert2::{assert, check},
    krabka_broker::coordinator::unified::GroupType,
    krabka_protocol::owned::{
        join_group_request::JoinGroupRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::time::Duration,
    tokio::time,
};

/// Parks the `JoinGroup` of one classic member of `group` on a connection of
/// its own, so that the member stays in the group, and waits until the classic
/// actor holds it.
///
/// A dynamic member first takes the `MEMBER_ID_REQUIRED` round. A static
/// member, with `instance_id`, joins at once.
async fn hold_live_classic_member(
    broker: &krabka_broker::BrokerHandle,
    bootstrap: &str,
    group: &str,
    instance_id: Option<&str>,
) {
    let member_id = if instance_id.is_none() {
        let client = connect(bootstrap).await;
        let r1 = time::timeout(Duration::from_secs(5), client.send(join_request(group, "")))
            .await
            .expect("JoinGroup1 timeout")
            .expect("JoinGroup1");
        assert!(
            r1.error_code == ERR_MEMBER_ID_REQUIRED,
            "expected MEMBER_ID_REQUIRED, got {r1:?}"
        );
        r1.member_id
    } else {
        String::new()
    };
    let request = JoinGroupRequest {
        group_instance_id: instance_id.map(str::to_owned),
        ..join_request(group, &member_id)
    };
    // The join parks in the rebalance-delay wait. The member stays joined (no
    // leave), so the group has a live member.
    let join_bootstrap = bootstrap.to_owned();
    tokio::spawn(async move {
        let c = connect_client(&join_bootstrap, Some("classic-joiner")).await;
        let _ = time::timeout(Duration::from_secs(30), c.send(request)).await;
    });
    broker.wait_until_classic_group_member_count(group, 1).await;
}

use rstest_bdd_macros::{given, scenario, then, when};

#[given("the classic upgrade group retains a live member with instance ID {instance}")]
async fn live_classic_upgrade_member(upgrade_case: &UpgradeScenario, instance: String) {
    hold_live_classic_member(
        upgrade_case.broker(),
        &upgrade_case.bootstrap,
        &upgrade_case.group,
        if instance == "absent" {
            None
        } else {
            Some(&instance)
        },
    )
    .await;
    assert!(
        upgrade_case
            .broker()
            .group_type_for_test(&upgrade_case.group)
            == Some(GroupType::Classic)
    );
}

#[when("a streams first-join heartbeat targets the occupied classic group")]
async fn refused_classic_upgrade(upgrade_case: &mut UpgradeScenario) {
    upgrade_case.response = Some(
        upgrade_case
            .streams()
            .send(first_join(
                &upgrade_case.group,
                topology(&upgrade_case.topic),
            ))
            .await
            .expect("StreamsGroupHeartbeat"),
    );
}

#[then("the occupied classic group returns the complete not-a-streams-group refusal")]
fn occupied_classic_upgrade_refusal(upgrade_case: &UpgradeScenario) {
    check!(
        upgrade_case
            .response
            .as_ref()
            .expect("streams heartbeat answered")
            == &StreamsGroupHeartbeatResponse {
                error_code: ERR_GROUP_ID_NOT_FOUND,
                error_message: Some(format!(
                    "Group {} is not a streams group.",
                    upgrade_case.group
                )),
                ..Default::default()
            }
    );
}

#[then("the occupied classic group retains its single live member")]
async fn occupied_classic_upgrade_unchanged(upgrade_case: &UpgradeScenario) {
    check!(
        upgrade_case
            .broker()
            .group_type_for_test(&upgrade_case.group)
            == Some(GroupType::Classic)
    );
    upgrade_case
        .broker()
        .wait_until_classic_group_member_count(&upgrade_case.group, 1)
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_upgrade/upgrade_live_member.feature",
    name = "classic group with live member rejects streams heartbeat"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_with_live_member_rejects_streams_heartbeat(
    upgrade_case: UpgradeScenario,
    group: String,
    instance: String,
) {
    let _ = (group, instance);
    upgrade_case.shutdown().await;
    drop(upgrade_case);
}
