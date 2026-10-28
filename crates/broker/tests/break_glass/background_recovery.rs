//! The background unclean-recovery path, under each of the three settings
//! `break_glass.background_unclean_recovery` takes.
//!
//! This path runs from leader election and from a broker heartbeat, with no
//! request, no connection and no principal, so a two-person rule cannot sit on
//! it. The table is what keeps the documented three-way choice honest: `off`
//! is today's behaviour, `audit-only` recovers and counts the bypass, and
//! `require` fails closed and leaves the partition visibly offline.

use {
    crate::{
        cluster::{index_of, plain_client, start_gated_cluster},
        support::{self, shutdown_cluster},
        topics::create_topic,
        transitions::bypassed,
    },
    assert2::check,
    krabka_broker::{BrokerHandle, NodeId, config::BackgroundUncleanRecovery},
    krabka_metadata::{
        BreakGlassAction as GatedAction, MetadataRecord, PartitionRecord, TopicConfigRecord,
    },
    std::{iter, time::Duration},
    tokio::time,
};

/// The topic the background cases take offline.
const RECOVERED: &str = "recovered";

/// Take partition 0 of [`RECOVERED`] offline behind the dead broker `victim`.
///
/// Every live ISR member is gone, so the controller's failover sweep hands the
/// partition to the offset-aware recovery manager with no proposal and nobody
/// to ask for one. That is the path this table is about, and it is the one path
/// a two-person rule cannot sit on.
async fn take_offline(controller: &BrokerHandle, victim: NodeId) {
    let current = controller
        .partition_record_for_test(RECOVERED, 0)
        .expect("a partition record");
    controller
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            leader: victim,
            isr: vec![victim],
            leader_epoch: current.leader_epoch.next(),
            partition_epoch: current.partition_epoch + 1,
            ..current.clone()
        }))
        .await
        .expect("take the partition offline");
}

use {
    assert2::assert,
    krabka_broker::BrokerConfig,
    krabka_client_core::Client,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct BackgroundRecovery {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    client: Option<Client>,
    controller: Option<NodeId>,
    victim: Option<NodeId>,
    stopped_directory: Option<TempDir>,
}

#[fixture]
fn recovery() -> BackgroundRecovery {
    // Start this scenario with fresh inputs and observations.

    BackgroundRecovery {
        cluster: Vec::new(),
        client: None,
        controller: None,
        victim: None,
        stopped_directory: None,
    }
}

#[given("a three-node broker cluster with background unclean recovery mode {mode}")]
async fn recovery_cluster(recovery: &mut BackgroundRecovery, mode: String) {
    let mode = match mode.as_str() {
        "off" => BackgroundUncleanRecovery::Off,
        "audit-only" => BackgroundUncleanRecovery::AuditOnly,
        "require" => BackgroundUncleanRecovery::Require,
        _ => panic!("unknown configured recovery mode {mode}"),
    };
    let cluster = start_gated_cluster(3, mode).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let leader = cluster[0].0.wait_until_controller_leader().await.unwrap();
    let victim = cluster
        .iter()
        .find(|(_, config, _)| config.node_id != leader)
        .expect("a non-controller")
        .1
        .node_id;
    recovery.client = Some(
        plain_client(
            &cluster[index_of(&cluster, leader)]
                .1
                .listen_addr
                .to_string(),
        )
        .await,
    );
    recovery.cluster = cluster;
    recovery.controller = Some(leader);
    recovery.victim = Some(victim);
}

#[given("RF3 topic recovered opts into strategy {strategy}")]
async fn recoverable_topic(recovery: &BackgroundRecovery, strategy: String) {
    create_topic(recovery.client.as_ref().unwrap(), RECOVERED, 3).await;
    for (handle, _, _) in &recovery.cluster {
        handle.wait_until_partition_present(RECOVERED, 0).await;
    }
    recovery.cluster[index_of(&recovery.cluster, recovery.controller.unwrap())]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: RECOVERED.to_owned(),
            overrides: iter::once(("unclean.recovery.strategy".to_owned(), strategy)).collect(),
        }))
        .await
        .expect("set recovery strategy");
}

#[when(
    "a non-controller broker dies and the partition names only that dead broker as leader and ISR"
)]
async fn take_partition_offline(recovery: &mut BackgroundRecovery) {
    let victim = recovery.victim.unwrap();
    let index = index_of(&recovery.cluster, victim);
    let (dead, _config, directory) = recovery.cluster.remove(index);
    dead.shutdown().await;
    recovery.stopped_directory = Some(directory);
    let controller = &recovery.cluster[index_of(&recovery.cluster, recovery.controller.unwrap())].0;
    controller
        .wait_for_image(|image| {
            image
                .partition(RECOVERED, 0)
                .is_some_and(|record| !record.isr.contains(&victim))
        })
        .await;
    take_offline(controller, victim).await;
}

#[then("a live recovery leader is elected {recovers} with offline observation window {window} ms")]
async fn elected(recovery: &BackgroundRecovery, recovers: bool, window: u64) {
    let controller = &recovery.cluster[index_of(&recovery.cluster, recovery.controller.unwrap())].0;
    let victim = recovery.victim.unwrap();
    if recovers {
        controller
            .wait_for_image(|image| {
                image
                    .partition(RECOVERED, 0)
                    .is_some_and(|record| record.leader != victim)
            })
            .await;
    } else {
        time::sleep(Duration::from_millis(window)).await;
    }
    let leader = controller
        .partition_record_for_test(RECOVERED, 0)
        .map(|record| record.leader);
    assert!(leader.is_some());
    check!((leader != Some(victim)) == recovers);
}

#[then("the unclean recovery bypass counter records an event {counted}")]
async fn bypass_counter(recovery: &BackgroundRecovery, counted: bool) {
    let controller = &recovery.cluster[index_of(&recovery.cluster, recovery.controller.unwrap())].0;
    if counted {
        controller
            .wait_for_metrics("counted recovery bypass", |_| {
                bypassed(controller, GatedAction::UncleanRecovery) >= 1
            })
            .await;
        check!(bypassed(controller, GatedAction::UncleanRecovery) >= 1);
    } else {
        check!(bypassed(controller, GatedAction::UncleanRecovery) == 0);
    }
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/background_recovery.feature",
    name = "background unclean recovery follows its configured rule"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_unclean_recovery_follows_its_configured_rule(
    recovery: BackgroundRecovery,
    mode: String,
    recovers: bool,
    counted: bool,
    window: u64,
) {
    let _ = (mode, recovers, counted, window);
    drop(recovery.client);
    shutdown_cluster(recovery.cluster).await;
}
