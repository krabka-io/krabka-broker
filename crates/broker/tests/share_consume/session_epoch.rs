//! The share-session state machine that sits in front of the acquire path.
//! The epoch checks reject a stale epoch and a member with no live session,
//! and an incremental request with no topic rows keeps fetching the cached
//! partitions until the client forgets one.

use assert2::assert;
use krabka_protocol::owned::{
    share_fetch_request::ForgottenTopic, share_fetch_response::ShareFetchResponse,
};

use crate::{
    ACCEPT, INVALID_SHARE_SESSION_EPOCH, NONE, ONE_MB, SHARE_SESSION_NOT_FOUND,
    harness::{join, produce_n, wire},
    share_rpc::{acquired_count, fetch_until_acquired, share_ack, share_fetch_req},
};

/// The share-session epoch state machine rejects stale and unknown epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_epoch_validation() {
    let crate::harness::ConsumptionFixture {
        _permit,
        broker: _broker,
        client,
        _dir,
        tid,
        member,
    } = crate::harness::consumption_fixture(1).await;

    // Open (epoch 0) succeeds: top-level error_code 0.
    let opened: ShareFetchResponse = client
        .send(share_fetch_req("g1", &member, tid, 0, 0, 0, vec![]))
        .await
        .expect("ShareFetch open");
    assert!(
        opened.error_code == NONE,
        "open (epoch 0) must succeed, got {}",
        opened.error_code
    );

    // The stored epoch is now 1. A non-matching positive epoch (say 9) →
    // INVALID_SHARE_SESSION_EPOCH (123) at the top level.
    let stale: ShareFetchResponse = client
        .send(share_fetch_req("g1", &member, tid, 0, 9, 0, vec![]))
        .await
        .expect("ShareFetch stale");
    assert!(
        stale.error_code == INVALID_SHARE_SESSION_EPOCH,
        "stale epoch must be 123 (INVALID_SHARE_SESSION_EPOCH), got {}",
        stale.error_code
    );

    // A member with no live session sending a non-zero epoch →
    // SHARE_SESSION_NOT_FOUND (122).
    let (ghost, _) = join(&client, "g1", "t").await;
    let not_found: ShareFetchResponse = client
        .send(share_fetch_req("g1", &ghost, tid, 0, 5, 0, vec![]))
        .await
        .expect("ShareFetch unknown session");
    assert!(
        not_found.error_code == SHARE_SESSION_NOT_FOUND,
        "unknown session must be 122 (SHARE_SESSION_NOT_FOUND), got {}",
        not_found.error_code
    );
}

/// An incremental request with no topic rows continues to fetch every cached
/// partition. A forgotten partition is removed from the session and stays
/// removed on later empty incremental requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_session_uses_cached_and_forgotten_partitions() {
    let crate::harness::ConsumptionFixture {
        _permit,
        broker: _broker,
        client,
        _dir,
        tid,
        member,
    } = crate::harness::consumption_fixture(1).await;

    let first = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&first) == 1);
    let first_ack = share_ack(&client, &member, tid, 1, 0, 0, ACCEPT).await;
    assert!(first_ack.error_code == NONE);

    produce_n(&client, "t", tid, 0, 1).await;
    let cached: ShareFetchResponse = client
        .send(crate::support::share::empty_session_request(
            "g1".into(),
            member.clone(),
            2,
            ONE_MB,
            vec![],
        ))
        .await
        .expect("incremental ShareFetch");
    assert!(cached.error_code == NONE);
    assert!(cached.responses.len() == 1);
    let cached_partition = &cached.responses[0].partitions[0];
    assert!(cached_partition.partition_index == 0);
    assert!(acquired_count(cached_partition) == 1);
    let second_ack = share_ack(&client, &member, tid, 3, 1, 1, ACCEPT).await;
    assert!(second_ack.error_code == NONE);

    let forgotten: ShareFetchResponse = client
        .send(crate::support::share::empty_session_request(
            "g1".into(),
            member.clone(),
            4,
            ONE_MB,
            vec![ForgottenTopic {
                topic_id: wire(tid),
                partitions: vec![0],
                ..Default::default()
            }],
        ))
        .await
        .expect("forget partition");
    assert!(forgotten.error_code == NONE);
    assert!(forgotten.responses.is_empty());

    produce_n(&client, "t", tid, 0, 1).await;
    let after_forget: ShareFetchResponse = client
        .send(crate::support::share::empty_session_request(
            "g1".into(),
            member,
            5,
            ONE_MB,
            vec![],
        ))
        .await
        .expect("ShareFetch after forget");
    assert!(after_forget.error_code == NONE);
    assert!(after_forget.responses.is_empty());
}
