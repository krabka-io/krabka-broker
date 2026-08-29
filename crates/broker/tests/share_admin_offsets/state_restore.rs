//! Test for the Slice F restore of `delivery_complete_count` across a restart.
//!
//! The cumulative `delivery_complete_count` is the basis for share-group lag,
//! so a recovered group that reset it to 0 would under-report its completed
//! work. The test lives apart from the admin-RPC surfaces because it asserts on
//! the recovered share-state summary rather than on a Describe, Alter, or
//! Delete response.

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker};

use crate::harness::{
    ACCEPT, NONE, ShareAck, acquired_count, bootstrap_share_state, broker_config,
    broker_test_permit, connect, create_topic, fetch_until_acquired, join, produce_n, share_ack,
    topic_id, wait_for_share_init,
};

/// F3, lag restore: `delivery_complete_count` survives a broker restart.
///
/// The cumulative `delivery_complete_count` is the number of
/// terminally-acknowledged records and is the basis for share-group lag. Before
/// Slice F, `load_from` reset it to 0, so the recovered group under-reported
/// its completed work.
///
/// Produce N. Consume and Accept all records, so the SPSO advances to N and
/// dcc = N. Wait for the persist. Restart on the same dir with Rejoin. Then
/// read the share-state summary. Its 4th element, `delivery_complete_count`,
/// must be the restored N, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_complete_count_restored_across_restart() {
    const N: i64 = 4;
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(broker_config(log_dir.clone())).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
        produce_n(&client, "t", tid, 0, N).await;
        let (member, _epoch) = join(&client, "g1", "t").await;
        wait_for_share_init(&broker, "g1", tid, 0).await;

        // Acquire 0..N-1 and Accept all → SPSO advances to N, dcc = N.
        let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
        assert!(acquired_count(&row) == N, "must acquire all {N} offsets");
        let ack = share_ack(
            &client,
            ShareAck {
                group: "g1",
                member: &member,
                topic_id: tid,
                partition: 0,
                epoch: 1,
                first: 0,
                last: N - 1,
                ack_type: ACCEPT,
            },
        )
        .await;
        assert!(ack.error_code == NONE, "accept error: {}", ack.error_code);

        // Wait until the persisted summary reflects dcc == N before restarting.
        broker
            .wait_until_share_delivery_complete("g1", tid, 0, i32::try_from(N).unwrap())
            .await;
        let dcc = broker
            .share_state_summary_for_test("g1", tid, 0)
            .await
            .map_or(-1, |(_, _, _, d)| d);
        assert!(
            dcc == i32::try_from(N).unwrap(),
            "pre-restart dcc must be {N}, got {dcc}"
        );

        // The awaiter above confirms dcc is durable; shut down immediately.
        broker.shutdown().await;
    }

    {
        let mut cfg = broker_config(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

        // The recovered summary must report the RESTORED dcc == N (not 0). The
        // summary load is driven by the share coordinator reading the persisted
        // record; await until the recovered state is present, then assert.
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        let summary = broker
            .share_state_summary_for_test("g1", tid, 0)
            .await
            .expect("summary present after wait_for_share_state_summary");
        let (_se, _le, start, dcc) = summary;
        // Sanity: the SPSO also recovered past the accepted records.
        assert!(start == N, "recovered SPSO must be {N}, got {start}");
        assert!(
            dcc == i32::try_from(N).unwrap(),
            "delivery_complete_count must be restored to {N} across restart, got {dcc}"
        );
    }
}
