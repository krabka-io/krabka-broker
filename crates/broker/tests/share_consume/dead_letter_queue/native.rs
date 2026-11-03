//! Native registration drives real broker rejects and shutdown, not a mock selector.

use {
    super::{
        DeadLetterShareVersion, GROUP, ShareRecordCount, acquired_records, broker_test_permit,
        configure_dead_letters, dead_letter_records,
    },
    async_trait::async_trait,
    krabka_broker::{
        Broker, BrokerError,
        share_partition::{
            DlqError, DlqManagerContext, DlqManagerFactory, DlqRequest, DlqSink, state::DlqCause,
        },
    },
    krabka_ids::Offset,
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
struct Observations {
    creates: AtomicUsize,
    stops: AtomicUsize,
    requests: Mutex<Vec<DlqRequest>>,
}
#[derive(Debug)]
struct Factory {
    observations: Arc<Observations>,
    fail_write: bool,
    reject_start: bool,
}
struct Native {
    observations: Arc<Observations>,
    stock: Arc<dyn DlqSink>,
    fail_write: bool,
}
impl DlqManagerFactory for Factory {
    fn create(&self, context: DlqManagerContext) -> Result<Arc<dyn DlqSink>, DlqError> {
        self.observations.creates.fetch_add(1, Ordering::SeqCst);
        if self.reject_start {
            return Err(DlqError::Config("native settings rejected".into()));
        }
        Ok(Arc::new(Native {
            observations: Arc::clone(&self.observations),
            stock: context.stock,
            fail_write: self.fail_write,
        }))
    }
}
#[async_trait]
impl DlqSink for Native {
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        if self.fail_write {
            Err(DlqError::Write("native backend refused".into()))
        } else {
            self.stock.write(request).await
        }
    }

    async fn stop(&self) {
        self.stock.stop().await;
        self.observations.stops.fetch_add(1, Ordering::SeqCst);
    }
}

#[yare::parameterized(stock_delegate={false}, native_rejection={true})]
#[test_macro(tokio::test)]
async fn registered_native_manager_receives_real_reject_and_closes_once(fail_write: bool) {
    let _permit = broker_test_permit().await;
    let observations = Arc::new(Observations::default());
    let factory = Arc::new(Factory {
        observations: Arc::clone(&observations),
        fail_write,
        reject_start: false,
    });
    let cluster = acquired_records(
        ShareRecordCount(3),
        move |config| {
            config.share_dlq_manager_factory = Some(factory);
        },
        async |client, broker| {
            configure_dead_letters(client, broker, DeadLetterShareVersion::Two, &[]).await;
        },
    )
    .await;
    cluster.reject_acquired(Offset(0), Offset(2)).await;
    cluster
        .broker
        .wait_until_share_spso(GROUP, cluster.tid, 0, 3)
        .await;
    let requests = observations.requests.lock().unwrap().clone();
    assert2::assert!(
        requests
            == vec![DlqRequest {
                group: GROUP.into(),
                topic_id: cluster.tid,
                source_partition: 0,
                first: Offset(0),
                last: Offset(2),
                delivery_count: 1,
                cause: DlqCause::ClientReject,
            }]
    );
    let records = dead_letter_records(&cluster.broker, &cluster.client).await;
    assert2::assert!(records.len() == if fail_write { 0 } else { 3 });
    cluster.broker.shutdown().await;
    assert2::assert!(
        (
            observations.creates.load(Ordering::SeqCst),
            observations.stops.load(Ordering::SeqCst)
        ) == (1, 1)
    );
}

#[tokio::test]
async fn invalid_native_registration_does_not_fall_back_to_stock() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = super::trunk_config(&dir);
    let observations = Arc::new(Observations::default());
    config.share_dlq_manager_factory = Some(Arc::new(Factory {
        observations: Arc::clone(&observations),
        fail_write: false,
        reject_start: true,
    }));
    assert2::assert!(
        matches!(Broker::start(config).await,Err(BrokerError::Startup(message)) if message.contains("native settings rejected"))
    );
    assert2::assert!(
        observations.creates.load(Ordering::SeqCst) == 1
            && observations.requests.lock().unwrap().is_empty()
    );
}
