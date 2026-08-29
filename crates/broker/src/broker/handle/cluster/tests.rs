use std::net::SocketAddr;

use assert2::{assert, check};
use tokio::net::TcpListener;

use super::*;
use crate::{broker::test_support::submit_metadata_topic_partition, config::BrokerConfig};

fn static_voter_test_config(
    log_dir: &std::path::Path,
    node_id: u64,
    listen_addr: SocketAddr,
    controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
    config.broker_id = i32::try_from(node_id).expect("node id fits broker id");
    config.node_id = krabka_raft::NodeId(node_id);
    config.listen_addr = listen_addr;
    config.advertised_listener = listen_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.directory_id = uuid::Uuid::from_u128(u128::from(node_id));
    config.controller_quorum_voters = voters
        .iter()
        .map(|(id, addr)| (krabka_raft::NodeId(*id), addr.to_string()))
        .collect();
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_handle_reports_non_default_node_and_voter_state() {
    let dir7 = tempfile::tempdir().unwrap();
    let dir8 = tempfile::tempdir().unwrap();
    let data_listener7 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_listener8 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_listener7 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_listener8 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen7 = data_listener7.local_addr().unwrap();
    let listen8 = data_listener8.local_addr().unwrap();
    let controller7 = controller_listener7.local_addr().unwrap();
    let controller8 = controller_listener8.local_addr().unwrap();
    let voters = [(7, controller7), (8, controller8)];

    let config7 = static_voter_test_config(dir7.path(), 7, listen7, controller7, &voters);
    let config8 = static_voter_test_config(dir8.path(), 8, listen8, controller8, &voters);
    let start = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(10),
        async {
            tokio::try_join!(
                Broker::start_with_listeners(
                    config7,
                    Some(controller_listener7),
                    Some(data_listener7),
                ),
                Broker::start_with_listeners(
                    config8,
                    Some(controller_listener8),
                    Some(data_listener8),
                ),
            )
        },
    ));
    let (handle7, handle8) = start
        .await
        .expect("two-voter brokers started before timeout")
        .expect("two-voter broker start");

    let leader = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(leader) = handle7.controller_leader_id()
                && leader != krabka_raft::NodeId(0)
            {
                return leader;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("two-voter cluster leader");
    assert!(leader == krabka_raft::NodeId(7) || leader == krabka_raft::NodeId(8));
    handle7.wait_for_image(|img| img.voters().len() == 2).await;
    handle8.wait_for_image(|img| img.voters().len() == 2).await;

    check!(handle7.node_id() == 7);
    check!(handle8.node_id() == 8);
    check!(handle7.controller_leader_id() == Some(leader));
    check!(
        handle7
            .quorum_voters_for_test()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            == [krabka_raft::NodeId(7), krabka_raft::NodeId(8)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
    );
    check!(handle7.voter_count_for_test() == 2);
    check!(
        handle7.voter_ids_for_test()
            == [krabka_raft::NodeId(7), krabka_raft::NodeId(8)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
    );

    // The multi-thread test runtime aborts remaining tasks on exit if raft
    // shutdown takes longer than the helper assertions above.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(handle7.shutdown(), handle8.shutdown());
    })
    .await;
}

#[tokio::test]
async fn wait_helpers_remain_pending_until_their_conditions_are_met() {
    type PendingWait<'a> = (
        &'a str,
        std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
    );
    type LeaderChangedCase<'a> = (&'a str, u128, u64, &'a [u64], i32, u64);

    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.expect("broker start");
    let timeout = std::time::Duration::from_millis(75);
    let topic_id = uuid::Uuid::from_u128(0xFEED);

    // Every wait helper must still be pending (time out) while its
    // condition is unmet. The futures are lazy async fns, so building the
    // table up front does no work; each is awaited sequentially below.
    let pending_waits: [PendingWait<'_>; 9] = [
        (
            "wait_for_share_state_summary",
            Box::pin(async {
                let () = handle
                    .wait_for_share_state_summary("missing-mutant-group", topic_id, 0)
                    .await;
            }),
        ),
        (
            "wait_until_share_spso",
            Box::pin(async {
                handle
                    .wait_until_share_spso("missing-mutant-group", topic_id, 0, 1)
                    .await;
            }),
        ),
        (
            "wait_until_share_delivery_complete",
            Box::pin(async {
                handle
                    .wait_until_share_delivery_complete("missing-mutant-group", topic_id, 0, 1)
                    .await;
            }),
        ),
        (
            "wait_until_group_member_count",
            Box::pin(async {
                handle
                    .wait_until_group_member_count("missing-mutant-group", 1)
                    .await;
            }),
        ),
        (
            "wait_until_streams_group_member_count",
            Box::pin(async {
                handle
                    .wait_until_streams_group_member_count("missing-mutant-streams", 1)
                    .await;
            }),
        ),
        (
            "wait_until_brokers_registered",
            Box::pin(async {
                handle.wait_until_brokers_registered(2).await;
            }),
        ),
        (
            "wait_until_partition_present",
            Box::pin(async {
                handle
                    .wait_until_partition_present("missing-mutant-topic", 0)
                    .await;
            }),
        ),
        (
            "wait_until_partition_leader_changed",
            Box::pin(async {
                handle
                    .wait_until_partition_leader_changed(
                        "missing-mutant-topic",
                        0,
                        krabka_raft::NodeId(1),
                    )
                    .await;
            }),
        ),
        (
            "wait_until_isr_len",
            Box::pin(async {
                handle
                    .wait_until_isr_len("missing-mutant-topic", 0, 1)
                    .await;
            }),
        ),
    ];
    for (name, wait) in pending_waits {
        assert!(
            tokio::time::timeout(timeout, wait).await.is_err(),
            "{name} resolved while its condition was unmet"
        );
    }

    // wait_until_partition_leader_changed must stay pending for each of
    // these submitted partitions:
    // (topic, topic_id, leader, replicas/isr, leader_epoch, excluded leader)
    let leader_changed_cases: [LeaderChangedCase<'_>; 4] = [
        // leader 0 means "no leader" — never counts as a change.
        ("leader-zero-mutant-topic", 0xF001, 0, &[1], 3, 1),
        // the current leader is exactly the excluded node.
        ("leader-excluded-mutant-topic", 0xF002, 2, &[1, 2], 3, 2),
        // leader epoch 0 is not a completed election.
        ("leader-epoch-zero-mutant-topic", 0xF003, 2, &[1, 2], 0, 1),
        // negative leader epoch likewise.
        (
            "leader-epoch-negative-mutant-topic",
            0xF004,
            2,
            &[1, 2],
            -1,
            1,
        ),
    ];
    for (topic, topic_id, leader, replicas, leader_epoch, excluded) in leader_changed_cases {
        submit_metadata_topic_partition(
            &handle,
            (topic, topic_id),
            0,
            leader,
            replicas,
            replicas,
            leader_epoch,
        )
        .await;
        assert!(
            tokio::time::timeout(
                timeout,
                handle.wait_until_partition_leader_changed(topic, 0, krabka_raft::NodeId(excluded)),
            )
            .await
            .is_err(),
            "{topic}: wait_until_partition_leader_changed resolved"
        );
    }
    // Leader 0 is also reported as "no leader" by the direct helper.
    assert!(
        handle
            .partition_leader_for_test("leader-zero-mutant-topic", 0)
            .is_none()
    );

    submit_metadata_topic_partition(
        &handle,
        ("isr-len-mutant-topic", 0xF005),
        0,
        1,
        &[1, 2],
        &[1, 2],
        3,
    )
    .await;
    assert!(
        tokio::time::timeout(
            timeout,
            handle.wait_until_isr_len("isr-len-mutant-topic", 0, 1)
        )
        .await
        .is_err()
    );

    handle.shutdown().await;
}
