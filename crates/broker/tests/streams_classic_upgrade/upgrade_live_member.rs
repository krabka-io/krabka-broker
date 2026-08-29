//! The rejection scenario: a classic group that still has a live member must
//! refuse the `StreamsGroupHeartbeat` with `GROUP_ID_NOT_FOUND` (69) and stay
//! Classic-typed.
//!
//! The scenario parks a `JoinGroup` on its own connection to hold the member
//! live, which is why it does not reuse the full `classic_join_sync` driver and
//! lives apart from the conversion scenario.

use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;

use crate::{
    upgrade_classic::join_request,
    upgrade_harness::{
        ERR_GROUP_ID_NOT_FOUND, ERR_MEMBER_ID_REQUIRED, boot, connect, create_topic,
        finalize_streams_version,
    },
    upgrade_streams::{first_join, topology},
};

/// A classic group with a **live** member rejects the `StreamsGroupHeartbeat`
/// with `GROUP_ID_NOT_FOUND` (69) and remains Classic-typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_with_live_member_rejects_streams_heartbeat() {
    let (broker, bootstrap, _dir) = boot().await;

    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in2", 1).await;

    // ── Phase 1: join as classic consumer and STAY (no leave). ──
    // First-round JoinGroup (gets member_id back).
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        classic_client.send(join_request("g2", "")),
    )
    .await
    .expect("JoinGroup1 timeout")
    .expect("JoinGroup1");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "expected MEMBER_ID_REQUIRED, got {r1:?}"
    );
    let member_id = r1.member_id.clone();

    // Second-round JoinGroup — parks in the rebalance-delay wait. We spawn it
    // so the test continues immediately without waiting for the park to return.
    // The member stays joined (no leave) so the group has a live member.
    let join_bootstrap = bootstrap.clone();
    let mid = member_id.clone();
    let _join_task = tokio::spawn(async move {
        let c = Client::builder()
            .bootstrap(&join_bootstrap)
            .client_id("classic-joiner")
            .build()
            .await
            .unwrap();
        let _ =
            tokio::time::timeout(Duration::from_secs(30), c.send(join_request("g2", &mid))).await;
    });

    // Wait for the member to land in the classic actor's member registry.
    broker.wait_until_classic_group_member_count("g2", 1).await;

    // Precondition: group must be Classic-typed.
    assert!(
        broker.group_type_for_test("g2")
            == Some(krabka_broker::coordinator::unified::GroupType::Classic),
        "precondition: group_type must be Classic, got {:?}",
        broker.group_type_for_test("g2")
    );

    // ── Phase 2: streams heartbeat for the same id must be rejected. ──
    let resp = streams_client
        .send(first_join("g2", topology("in2")))
        .await
        .expect("StreamsGroupHeartbeat");
    assert!(
        resp.error_code == ERR_GROUP_ID_NOT_FOUND,
        "streams heartbeat for classic group with live member must return \
         GROUP_ID_NOT_FOUND (69), got error_code={}",
        resp.error_code
    );

    // Group must STILL be Classic-typed (no flip).
    assert!(
        broker.group_type_for_test("g2")
            == Some(krabka_broker::coordinator::unified::GroupType::Classic),
        "group_type must remain Classic after rejected upgrade, got {:?}",
        broker.group_type_for_test("g2")
    );
}
