//! The controller engine's event-queue measurements: [`BrokerMetrics`] is the
//! [`EventQueueRecorder`] a controller or combined node hands its engine.

use {super::BrokerMetrics, krabka_raft::EventQueueRecorder, std::time::Duration};

impl EventQueueRecorder for BrokerMetrics {
    fn queue_time(&self, waited: Duration) {
        self.controller_event_queue_time_seconds
            .observe(waited.as_secs_f64());
    }

    fn processing_time(&self, took: Duration) {
        self.controller_event_queue_processing_time_seconds
            .observe(took.as_secs_f64());
    }

    fn operation_started(&self) { self.controller_event_queue_operations_started_total.inc(); }

    fn operation_timed_out(&self) { self.controller_event_queue_operations_timed_out_total.inc(); }

    fn idle_time(&self, idle: Duration) {
        self.controller_event_queue_idle_seconds_total
            .inc_by(idle.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::metrics::test_support::encode_metrics, assert2::check,
        rstest_bdd::datatable::Rows,
    };

    /// The event-queue series one scrape shows.
    #[derive(Debug, Default, PartialEq)]
    struct EventQueueScrape {
        queue_count: f64,
        queue_sum: f64,
        processing_count: f64,
        processing_sum: f64,
        started: f64,
        timed_out: f64,
        idle_seconds: f64,
    }

    impl EventQueueScrape {
        fn read(metrics: &BrokerMetrics) -> Self {
            let body = encode_metrics(metrics);
            let sample = |name: &str| {
                body.lines()
                    .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(f64::NAN)
            };
            Self {
                queue_count: sample("krabka_broker_controller_event_queue_time_seconds_count"),
                queue_sum: sample("krabka_broker_controller_event_queue_time_seconds_sum"),
                processing_count: sample(
                    "krabka_broker_controller_event_queue_processing_time_seconds_count",
                ),
                processing_sum: sample(
                    "krabka_broker_controller_event_queue_processing_time_seconds_sum",
                ),
                started: sample("krabka_broker_controller_event_queue_operations_started_total"),
                timed_out: sample(
                    "krabka_broker_controller_event_queue_operations_timed_out_total",
                ),
                idle_seconds: sample("krabka_broker_controller_event_queue_idle_seconds_total"),
            }
        }
    }

    struct ControllerQueueMetrics {
        metrics: BrokerMetrics,
        scrape: Option<EventQueueScrape>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ExpectedQueueMetrics {
        queue_count: f64,
        queue_sum: f64,
        processing_count: f64,
        processing_sum: f64,
        started: f64,
        timed_out: f64,
        idle_seconds: f64,
    }

    #[rstest::fixture]
    fn controller_queue_metrics() -> ControllerQueueMetrics {
        // Start this scenario with fresh inputs and observations.

        ControllerQueueMetrics {
            metrics: BrokerMetrics::new(),
            scrape: None,
        }
    }

    #[rstest_bdd_macros::given("a fresh controller event queue metrics registry")]
    fn fresh_controller_queue_metrics(controller_queue_metrics: &mut ControllerQueueMetrics) {
        controller_queue_metrics.metrics = BrokerMetrics::new();
    }

    #[rstest_bdd_macros::when(
        "a controller operation waits {milliseconds} milliseconds in the queue"
    )]
    fn record_queue_wait(controller_queue_metrics: &ControllerQueueMetrics, milliseconds: u64) {
        controller_queue_metrics
            .metrics
            .queue_time(Duration::from_millis(milliseconds));
    }

    #[rstest_bdd_macros::when(
        "a controller operation takes {milliseconds} milliseconds to process"
    )]
    fn record_queue_processing(
        controller_queue_metrics: &ControllerQueueMetrics,
        milliseconds: u64,
    ) {
        controller_queue_metrics
            .metrics
            .processing_time(Duration::from_millis(milliseconds));
    }

    #[rstest_bdd_macros::when("a controller operation starts")]
    fn record_queue_operation(controller_queue_metrics: &ControllerQueueMetrics) {
        controller_queue_metrics.metrics.operation_started();
    }

    #[rstest_bdd_macros::when("a controller operation times out")]
    fn record_queue_timeout(controller_queue_metrics: &ControllerQueueMetrics) {
        controller_queue_metrics.metrics.operation_timed_out();
    }

    #[rstest_bdd_macros::when("the controller queue is idle for {milliseconds} milliseconds")]
    fn record_queue_idle(controller_queue_metrics: &ControllerQueueMetrics, milliseconds: u64) {
        controller_queue_metrics
            .metrics
            .idle_time(Duration::from_millis(milliseconds));
    }

    #[rstest_bdd_macros::when("the Kafka named controller queue series are scraped")]
    fn scrape_controller_queue(controller_queue_metrics: &mut ControllerQueueMetrics) {
        controller_queue_metrics.scrape =
            Some(EventQueueScrape::read(&controller_queue_metrics.metrics));
    }

    #[rstest_bdd_macros::then("the controller event queue metrics contain:")]
    fn expected_controller_queue_metrics(
        controller_queue_metrics: &ControllerQueueMetrics,
        #[datatable] rows: Rows<ExpectedQueueMetrics>,
    ) {
        let mut rows = rows.into_iter();
        let row = rows.next().expect("one independent expected metric sample");
        check!(rows.next().is_none());
        check!(
            controller_queue_metrics.scrape.as_ref()
                == Some(&EventQueueScrape {
                    queue_count: row.queue_count,
                    queue_sum: row.queue_sum,
                    processing_count: row.processing_count,
                    processing_sum: row.processing_sum,
                    started: row.started,
                    timed_out: row.timed_out,
                    idle_seconds: row.idle_seconds
                })
        );
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/metrics/event_queue.feature",
        fixtures = [controller_queue_metrics: ControllerQueueMetrics]
    );
}
