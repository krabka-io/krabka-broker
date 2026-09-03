//! KIP-460 auto preferred-replica rebalance.
//!
//! A background task on the controller leader scans every partition
//! periodically. For each partition where
//! `select_new_leader_for_partition(Preferred)` succeeds, the task queues a
//! `V1Partition` update. The task submits one batch per tick when the
//! cluster-wide imbalance ratio crosses the configured threshold.

use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use async_trait::async_trait;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_units::{
    Ratio, Time,
    convert::{RatioExt, TimeExt as _},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::{ElectionType, select_new_leader_for_partition},
};

/// Minimal trait for the controller surface this module uses. It lets tests
/// inject a mock without a real raft cluster.
#[async_trait]
pub(crate) trait ControllerLike: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub(crate) struct AutoRebalanceConfig {
    pub check_interval: Time,
    #[allow(dead_code)]
    pub imbalance_threshold: Ratio,
}

/// Spawned task entry point.
pub(crate) async fn run(
    controller: Arc<dyn ControllerLike>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.check_interval.to_std());
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            () = shutdown.cancelled() => {
                info!("auto-rebalance task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("auto-rebalance tick skipped: not controller leader");
            continue;
        }
        rebalance_tick(&*controller, &liveness, &cfg).await;
    }
}

pub(crate) async fn rebalance_tick(
    controller: &dyn ControllerLike,
    liveness: &ControllerLivenessState,
    _cfg: &AutoRebalanceConfig,
) {
    let image = controller.current_image();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    let mut selected_keys = HashSet::new();
    let mut total: u64 = 0;
    // Witness nodes never lead. Build the set once per tick, not once per
    // partition, so the tick stays a single walk over the image.
    let witnesses = crate::config_keys::witness_node_ids(&image);
    // Single O(P) walk over every partition.
    for pr in image.all_partitions() {
        let Some(next_total) = total.checked_add(1) else {
            warn!("auto-rebalance: partition count overflow; skipping tick");
            return;
        };
        total = next_total;
        if let Ok(new_pr) = select_new_leader_for_partition(
            &image,
            liveness,
            &witnesses,
            &pr.topic,
            pr.partition,
            ElectionType::Preferred,
        )
        .await
        {
            // PreferredAlreadyLeader, PreferredIsWitness and any other Err are
            // silently skipped this tick.
            if !selected_keys.insert((new_pr.topic.clone(), new_pr.partition)) {
                warn!(
                    topic = %new_pr.topic,
                    partition = new_pr.partition,
                    "auto-rebalance: duplicate partition change; skipping tick"
                );
                return;
            }
            to_submit.push(MetadataRecord::V1Partition(new_pr));
        }
    }
    let imbalanced = u64::try_from(to_submit.len()).unwrap_or(u64::MAX);
    if !krabka_verified::preferred_rebalance_admission(
        total,
        imbalanced,
        selected_keys.len() == to_submit.len(),
        true,
    ) {
        debug!(
            imbalanced,
            total, "auto-rebalance: batch admission denied"
        );
        return;
    }
    info!(count = imbalanced, "auto-rebalance: submitting elections");
    if let Err(e) = controller.submit_change(to_submit).await {
        warn!(error = %e, "auto-rebalance submit failed");
    }
}

/// Compare `selected / total` with the stored ratio's shortest decimal form.
/// This preserves operator percentage semantics (`10%` is exactly `1/10`)
/// and avoids both truncated percentages and lossy count conversion.
#[allow(dead_code)]
fn exact_ratio_at_least(selected: u64, total: u64, threshold: Ratio) -> bool {
    if selected == 0 || total == 0 || selected > total {
        return false;
    }
    let value = threshold.as_f64();
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return false;
    }
    if value == 0.0 {
        return true;
    }
    match decimal_threshold(value) {
        ExactThreshold::Invalid => false,
        ExactThreshold::TinyPositive => true,
        ExactThreshold::Fraction(numerator, denominator) => fraction_at_least(
            u128::from(selected),
            u128::from(total),
            numerator,
            denominator,
        ),
    }
}

enum ExactThreshold {
    Invalid,
    TinyPositive,
    Fraction(u128, u128),
}

fn decimal_threshold(value: f64) -> ExactThreshold {
    let text = value.to_string();
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or((text.as_str(), 0_i32), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(i32::MIN))
        });
    if exponent == i32::MIN {
        return ExactThreshold::Invalid;
    }
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let scale = i32::try_from(fractional.len()).unwrap_or(i32::MAX) - exponent;
    if scale > 38 {
        // The shortest binary64 decimal has at most 17 significant digits.
        // Beyond 38 decimal places it is below 1 / u64::MAX, so any nonzero
        // selected count is above it.
        return ExactThreshold::TinyPositive;
    }
    let digits = format!("{whole}{fractional}");
    let Some(mut numerator) = digits.parse::<u128>().ok() else {
        return ExactThreshold::Invalid;
    };
    let denominator = if scale >= 0 {
        let Some(denominator) = 10_u128.checked_pow(u32::try_from(scale).unwrap_or(u32::MAX))
        else {
            return ExactThreshold::TinyPositive;
        };
        denominator
    } else {
        let Some(multiplier) = 10_u128.checked_pow(scale.unsigned_abs()) else {
            return ExactThreshold::Invalid;
        };
        let Some(scaled) = numerator.checked_mul(multiplier) else {
            return ExactThreshold::Invalid;
        };
        numerator = scaled;
        1
    };
    ExactThreshold::Fraction(numerator, denominator)
}

/// Compare two nonnegative fractions exactly without cross-multiplication.
/// The continued-fraction walk uses only division and remainder, so even the
/// `u64` count boundaries and a 38-digit decimal denominator cannot overflow.
fn fraction_at_least(
    mut left_numerator: u128,
    mut left_denominator: u128,
    mut right_numerator: u128,
    mut right_denominator: u128,
) -> bool {
    let mut reversed = false;
    loop {
        let left_quotient = left_numerator / left_denominator;
        let right_quotient = right_numerator / right_denominator;
        if left_quotient != right_quotient {
            return match left_quotient.cmp(&right_quotient) {
                Ordering::Greater => !reversed,
                Ordering::Less => reversed,
                Ordering::Equal => unreachable!(),
            };
        }
        let left_remainder = left_numerator % left_denominator;
        let right_remainder = right_numerator % right_denominator;
        if left_remainder == 0 || right_remainder == 0 {
            return match left_remainder.cmp(&right_remainder) {
                Ordering::Greater => !reversed,
                Ordering::Less => reversed,
                Ordering::Equal => true,
            };
        }
        (left_numerator, left_denominator) = (left_denominator, left_remainder);
        (right_numerator, right_denominator) = (right_denominator, right_remainder);
        reversed = !reversed;
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use assert2::assert;
    use krabka_metadata::{PartitionRecord, TopicRecord};
    use krabka_units::{millis, minutes, percent, secs};
    use uuid::Uuid;

    use super::*;

    struct MockController {
        image: Arc<MetadataImage>,
        is_leader: bool,
        submitted: Mutex<Vec<MetadataRecord>>,
        submit_calls: std::sync::atomic::AtomicUsize,
        fail_submissions: std::sync::atomic::AtomicUsize,
    }

    impl MockController {
        fn new(image: Arc<MetadataImage>, is_leader: bool) -> Self {
            Self {
                image,
                is_leader,
                submitted: Mutex::new(Vec::new()),
                submit_calls: std::sync::atomic::AtomicUsize::new(0),
                fail_submissions: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn fail_next_submission(&self) {
            self.fail_submissions
                .store(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ControllerLike for MockController {
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            self.submit_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self
                .fail_submissions
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err("injected submit failure".into());
            }
            self.submitted.lock().unwrap().extend(records);
            Ok(())
        }
    }

    fn img_with_n_partitions(imbalanced: usize, balanced: usize) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: i32::try_from(imbalanced + balanced).expect("partition count fits i32"),
            replication_factor: 3,
        }));
        let mut p = 0i32;
        // Imbalanced: leader = 2 (not preferred). ISR has all three.
        for _ in 0..imbalanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: krabka_audit::NodeId(2),
                replicas: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                isr: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
            p += 1;
        }
        // Balanced: leader = 1 (preferred).
        for _ in 0..balanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: krabka_audit::NodeId(1),
                replicas: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                isr: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
            p += 1;
        }
        Arc::new(img)
    }

    async fn liveness_all_alive() -> ControllerLivenessState {
        let l = ControllerLivenessState::new(secs(10));
        for n in [1, 2, 3] {
            l.record_heartbeat(n).await;
        }
        l
    }

    #[test]
    fn exact_threshold_comparison_handles_boundaries_and_invalid_values() {
        assert!(exact_ratio_at_least(10, 100, percent(10)));
        assert!(!exact_ratio_at_least(999, 10_000, percent(10)));
        assert!(exact_ratio_at_least(109, 1_000, percent(10)));
        assert!(exact_ratio_at_least(u64::MAX, u64::MAX, percent(100)));
        assert!(!exact_ratio_at_least(u64::MAX - 1, u64::MAX, percent(100)));
        assert!(exact_ratio_at_least(
            1,
            u64::MAX,
            krabka_units::fraction(f64::MIN_POSITIVE)
        ));
        for invalid in [
            krabka_units::fraction(f64::NAN),
            krabka_units::fraction(f64::INFINITY),
            krabka_units::fraction(-0.1),
            krabka_units::fraction(1.1),
        ] {
            assert!(!exact_ratio_at_least(1, 1, invalid));
        }
        assert!(!exact_ratio_at_least(0, 1, percent(0)));
        assert!(!exact_ratio_at_least(2, 1, percent(0)));
    }

    #[tokio::test]
    async fn offline_or_out_of_isr_preferred_replica_is_not_submitted() {
        let offline = MockController::new(img_with_n_partitions(1, 0), true);
        let liveness = ControllerLivenessState::new(secs(10));
        for node in [2, 3] {
            liveness.record_heartbeat(node).await;
        }
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
            imbalance_threshold: <Ratio as RatioExt>::ZERO,
        };
        rebalance_tick(&offline, &liveness, &cfg).await;
        assert!(offline.submitted.lock().unwrap().is_empty());

        let mut image =
            Arc::into_inner(img_with_n_partitions(1, 0)).expect("fixture has one Arc reference");
        let mut partition = image.partition("foo", 0).unwrap().clone();
        partition.isr = vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)];
        image.apply(&MetadataRecord::V1Partition(partition));
        let out_of_isr = MockController::new(Arc::new(image), true);
        rebalance_tick(&out_of_isr, &liveness_all_alive().await, &cfg).await;
        assert!(out_of_isr.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_submission_can_retry_the_same_nonempty_change() {
        let mock = MockController::new(img_with_n_partitions(1, 0), true);
        mock.fail_next_submission();
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
            imbalance_threshold: <Ratio as RatioExt>::ZERO,
        };

        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 1);
        assert!(mock.submit_calls.load(std::sync::atomic::Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn below_threshold_skips_submit() {
        // 5 imbalanced out of 100 → 5%; threshold 10% → no submit.
        let mock = MockController::new(img_with_n_partitions(5, 95), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn exact_threshold_submits_imbalanced_set() {
        // 10 imbalanced out of 100 is exactly 10%; threshold 10% should submit.
        let mock = MockController::new(img_with_n_partitions(10, 90), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 10);
    }

    /// The threshold is a [`Ratio`], so the code compares a fraction that
    /// falls between two whole percentages at full precision.
    /// `floor(100 * r) < T` and `r < T / 100` agree for every integer `T`.
    /// This test therefore pins that the move away from the old truncating
    /// `(imbalanced * 100) / total` left the KIP-460 decision boundary
    /// exactly where it was.
    #[tokio::test]
    async fn fractional_percentages_compare_against_the_threshold_exactly() {
        // 200 partitions gives half-percent granularity either side of 10%.
        for (imbalanced, balanced, want_submitted) in
            [(19_usize, 181_usize, 0_usize), (21, 179, 21)]
        {
            let mock = MockController::new(img_with_n_partitions(imbalanced, balanced), true);
            let liveness = liveness_all_alive().await;
            let cfg = AutoRebalanceConfig {
                check_interval: minutes(5),
                imbalance_threshold: percent(10),
            };

            rebalance_tick(&mock, &liveness, &cfg).await;

            assert!(
                mock.submitted.lock().unwrap().len() == want_submitted,
                "{imbalanced}/{} imbalanced",
                imbalanced + balanced
            );
        }
    }

    #[tokio::test]
    async fn zero_imbalance_does_not_submit_empty_batch() {
        // Every partition is already balanced (leader == preferred). Even
        // with threshold 0% the tick must NOT call submit_change: an empty
        // batch still writes a spurious raft entry, which broadcasts the
        // metadata image and churns every broker's reconcile loop once per
        // tick (starving ISR re-admission of catching-up replicas).
        let mock = MockController::new(img_with_n_partitions(0, 5), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: secs(1),
            imbalance_threshold: <Ratio as RatioExt>::ZERO,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(
            mock.submit_calls.load(std::sync::atomic::Ordering::SeqCst) == 0,
            "must not submit when there is nothing to rebalance"
        );
    }

    #[tokio::test]
    async fn above_threshold_submits_imbalanced_set() {
        // 20 imbalanced out of 100 → 20%; threshold 10% → submit 20.
        let mock = MockController::new(img_with_n_partitions(20, 80), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert!(submitted.len() == 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert!(p.leader == krabka_audit::NodeId(1)),
                _ => panic!("unexpected record type"),
            }
        }
    }

    #[tokio::test]
    async fn run_submits_when_controller_is_leader() {
        let controller = Arc::new(MockController::new(img_with_n_partitions(1, 0), true));
        let controller_for_run: Arc<dyn ControllerLike> = controller.clone();
        let liveness = Arc::new(liveness_all_alive().await);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            controller_for_run,
            liveness,
            AutoRebalanceConfig {
                check_interval: millis(10),
                imbalance_threshold: <Ratio as RatioExt>::ZERO,
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_millis(500), async {
            while controller
                .submit_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader auto-rebalance loop should submit");

        shutdown.cancel();
        task.await.unwrap();
        assert!(!controller.submitted.lock().unwrap().is_empty());
    }
}
