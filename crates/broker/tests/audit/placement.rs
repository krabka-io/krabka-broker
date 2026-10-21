//! The placement of `__krabka_audit` in a three-node combined cluster.
//!
//! Every node runs the controller and the broker, and the cluster default of
//! `min.insync.replicas` is 2. The nodes start together, so the quorum leader
//! often creates the topic before the others register. Each broker must still
//! end up leading one audit partition of its own, and no audit partition may
//! count as under min ISR.

use {
    crate::support,
    assert2::{assert, check},
    krabka_broker::{NodeId, coordinator::AUDIT_TOPIC},
    std::collections::BTreeMap,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_combined_node_leads_one_audit_partition_with_a_minimum_isr_of_one() {
    let cluster = support::start_n_node_customized_with_retry(
        3,
        |_, config| {
            config.default_min_insync_replicas = 2;
            config.gauge_poll_interval = krabka_units::millis(100);
        },
        "combined audit cluster",
    )
    .await;
    for (handle, config, _) in &cluster {
        handle
            .wait_for_image(|image| image.partitions_of(AUDIT_TOPIC).count() == 3)
            .await;
        let image = handle.controller_image_for_test();
        let mut placement: Vec<_> = image
            .partitions_of(AUDIT_TOPIC)
            .map(|record| (record.leader, record.replicas.clone(), record.isr.clone()))
            .collect();
        placement.sort();
        let led = image
            .partitions_of(AUDIT_TOPIC)
            .find(|record| record.leader == config.node_id)
            .map(|record| record.partition)
            .expect("this broker leads an audit partition");
        // Each broker's audit writer records its own start in its partition.
        handle
            .wait_until_local_log_end_offset(AUDIT_TOPIC, led, 1)
            .await;
        // A gauge pass that starts after the first record landed has seen the
        // whole placement.
        handle
            .wait_for_metrics("a gauge pass over the audit placement", |metrics| {
                metrics.partitions_led.get() >= 1
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        check!(
            placement
                == [1, 2, 3]
                    .map(|node| (NodeId(node), vec![NodeId(node)], vec![NodeId(node)]))
                    .to_vec(),
            "node {}",
            config.node_id.0
        );
        let mut expected_config =
            BTreeMap::from([("min.insync.replicas".to_owned(), "1".to_owned())]);
        for node in 1..=3 {
            expected_config.insert(
                format!("krabka.audit.placement.epoch.{node}"),
                image
                    .broker_epoch(NodeId(node))
                    .expect("registered broker")
                    .to_string(),
            );
        }
        check!(
            image.topic_config(AUDIT_TOPIC) == Some(&expected_config),
            "node {}",
            config.node_id.0
        );
        assert!(
            handle.metrics().under_min_isr_partition_count.get() == 0,
            "node {}",
            config.node_id.0
        );
    }

    support::shutdown_cluster(cluster).await;
}
