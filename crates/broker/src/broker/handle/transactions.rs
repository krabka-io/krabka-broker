//! Test-only [`BrokerHandle`] helpers that control the transaction
//! coordinator's marker fan-out, so a test can stop or fail a transaction
//! after its `Prepare*` record is durable.

use crate::{broker::BrokerHandle, txn::coordinator::fanout_gate::MarkerFanoutMode};

impl BrokerHandle {
    /// Test-only: set what every transaction-marker fan-out does at its start.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn set_transaction_marker_fanout_for_test(&self, mode: MarkerFanoutMode) {
        self.broker
            .txn_coordinator
            .marker_fanout_gate
            .set_mode(mode);
    }

    /// Test-only: wait until `count` marker fan-outs reached a gate that was
    /// not open.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_for_transaction_marker_fanouts_for_test(&self, count: usize) {
        let arrived = tokio::time::timeout(
            crate::broker::TEST_AWAITER_TIMEOUT,
            self.broker
                .txn_coordinator
                .marker_fanout_gate
                .wait_for_arrivals(count),
        )
        .await;
        assert2::assert!(
            arrived.is_ok(),
            "{count} transaction marker fan-outs did not reach the test gate"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use assert2::assert;

    use crate::{
        broker::{Broker, BrokerConfig},
        txn::coordinator::fanout_gate::MarkerFanoutMode,
    };

    #[tokio::test]
    async fn marker_fanout_helpers_set_mode_and_wait_for_arrivals() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        handle.set_transaction_marker_fanout_for_test(MarkerFanoutMode::Hold);
        let arrived = Arc::new(AtomicBool::new(false));
        let arrival_flag = Arc::clone(&arrived);
        let broker_ref = Arc::clone(&broker);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            arrival_flag.store(true, Ordering::Release);
            let _ = broker_ref.txn_coordinator.marker_fanout_gate.pass().await;
        });

        handle.wait_for_transaction_marker_fanouts_for_test(1).await;
        assert!(arrived.load(Ordering::Acquire));

        handle.set_transaction_marker_fanout_for_test(MarkerFanoutMode::Fail);
        assert!(
            broker
                .txn_coordinator
                .marker_fanout_gate
                .pass()
                .await
                .is_err()
        );

        handle.set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
        let _ = task.await;
        handle.shutdown().await;
    }
}
