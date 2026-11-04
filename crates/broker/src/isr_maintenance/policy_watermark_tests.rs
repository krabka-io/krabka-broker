//! A policy-only scan must publish already-durable records after follower fencing.
use {
    super::{
        proposal::{Proposal, compute_proposal},
        test_support::partition,
    },
    crate::{
        partition::{Partition, test_support::test_partition},
        replica_state::{BrokerStanding, LeaderPolicy},
        test_support::{RecordCount, partition_records_batch},
    },
    assert2::{assert, check},
    krabka_ids::LeaderEpoch,
    krabka_log::Offset,
    krabka_metadata::{MetadataRecord, NodeId},
    std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{runtime::Runtime, sync::Notify},
};

#[derive(Default)]
struct PolicyRefresh {
    storage: String,
    observed: Option<(i64, bool, bool)>,
}

#[rstest::fixture]
fn policy_refresh() -> PolicyRefresh {
    // Each scenario owns its own partition and eligibility observation.
    PolicyRefresh::default()
}

#[rstest_bdd_macros::given("a {storage} leader whose recently caught-up follower has left the ISR")]
fn given_leader(policy_refresh: &mut PolicyRefresh, storage: String) {
    policy_refresh.storage = storage;
}

async fn waiting_leader(storage: &str) -> (Partition, TempDir, LeaderPolicy) {
    let (mut part, dir) = test_partition(Arc::new(Notify::new()));
    part.diskless = storage == "diskless";
    part.install_replication_target(None, 1, 0).await;
    let replicas = [NodeId(1), NodeId(2)];
    part.install_isr(&replicas, &replicas, NodeId(1)).await;
    let standing = BrokerStanding {
        fenced: false,
        shutting_down: false,
        alive_epoch: Some(5),
    };
    let old_policy = LeaderPolicy {
        effective_min_isr: 1,
        replica_lag_time_max: Duration::from_secs(60),
        brokers: HashMap::from([(NodeId(1), standing), (NodeId(2), standing)]),
    };
    {
        let mut state = part.replica_state.lock().await;
        state.set_policy(old_policy.clone());
        state.record_follower_broker_epoch(NodeId(2), 5);
        state.update_follower_leo(NodeId(2), Offset(0), Offset(0), Instant::now());
    }
    part.log
        .lock()
        .unwrap()
        .assign_epoch_start_offset(LeaderEpoch(0), Offset(0))
        .unwrap();
    let mut batch = partition_records_batch().count(RecordCount(3)).call();
    part.log.lock().unwrap().append(&mut batch).unwrap();
    part.install_isr(&[NodeId(1)], &replicas, NodeId(1)).await;
    assert!(part.high_watermark().await == Offset(0));
    (part, dir, old_policy)
}

#[rstest_bdd_macros::when("the scan refreshes policy while respecting current durability")]
fn refresh_policy(policy_refresh: &mut PolicyRefresh) {
    let storage = policy_refresh.storage.clone();
    policy_refresh.observed = Some(Runtime::new().unwrap().block_on(async {
        let (part, _dir, old_policy) = waiting_leader(&storage).await;
        if storage == "superseded" {
            part.install_replication_target(None, 2, 1).await;
        }
        let waiter = part.hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(futures_util::poll!(&mut waiter).is_pending());
        let mut new_policy = old_policy;
        match storage.as_str() {
            "expired" => {
                let mut state = part.replica_state.lock().await;
                state
                    .per_follower
                    .get_mut(&NodeId(2))
                    .unwrap()
                    .last_caught_up =
                    Some(Instant::now().checked_sub(Duration::from_secs(61)).unwrap());
            }
            "eligible" => {}
            "pending-expansion" => {
                let mut state = part.replica_state.lock().await;
                let follower = state.per_follower.get_mut(&NodeId(2)).unwrap();
                follower.last_caught_up =
                    Some(Instant::now().checked_sub(Duration::from_secs(61)).unwrap());
                follower.fetched_since_isr_exit = true;
                drop(state);
            }
            _ => {
                new_policy.brokers.insert(NodeId(2), BrokerStanding {
                    fenced: true,
                    shutting_down: false,
                    alive_epoch: None,
                });
            }
        }
        if storage == "below-minimum" {
            new_policy.effective_min_isr = 2;
        }
        let MetadataRecord::V1Partition(mut record) = partition()
            .isr(&[NodeId(1)])
            .replicas(&[NodeId(1), NodeId(2)])
            .call()
        else {
            unreachable!()
        };
        if storage == "uninstalled-isr" {
            record.isr = vec![NodeId(1), NodeId(2)];
        }
        if storage == "concurrent-append" {
            let mut state = part.replica_state.lock().await;
            let refresh = compute_proposal(&part, &record, new_policy);
            tokio::pin!(refresh);
            assert!(futures_util::poll!(&mut refresh).is_pending());
            let mut next = partition_records_batch().count(RecordCount(1)).call();
            part.log.lock().unwrap().append(&mut next).unwrap();
            state.hw = Offset(4);
            drop(state);
            assert!(refresh.await.is_none());
        } else if storage == "pending-expansion" {
            assert!(
                compute_proposal(&part, &record, new_policy).await
                    == Some(Proposal {
                        prev_isr: vec![NodeId(1)],
                        new_isr: vec![NodeId(1), NodeId(2)],
                        leader_epoch: LeaderEpoch(0),
                        partition_epoch: 0,
                    })
            );
        } else {
            assert!(compute_proposal(&part, &record, new_policy).await.is_none());
        }
        let notified = futures_util::poll!(&mut waiter).is_ready();
        let committed = part
            .await_hw_at_least(Offset(3), Instant::now())
            .await
            .is_ok();
        (part.high_watermark().await.0, notified, committed)
    }));
}

#[rstest_bdd_macros::then(
    "the high watermark is {watermark}, notification is {notified} and all-acks completion is {committed}"
)]
fn observe_policy(policy_refresh: &PolicyRefresh, watermark: i64, notified: bool, committed: bool) {
    check!(policy_refresh.observed == Some((watermark, notified, committed)));
}

rstest_bdd_macros::scenarios!("tests/features/isr_policy_refresh/policy.feature", fixtures = [policy_refresh: PolicyRefresh]);
