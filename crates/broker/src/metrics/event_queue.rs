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
    use {super::*, crate::metrics::test_support::encode_metrics, assert2::check};

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

    /// One recorder call and the scrape it leaves on a fresh registry.
    struct RecorderCase {
        name: &'static str,
        record: fn(&BrokerMetrics),
        expected: EventQueueScrape,
    }

    #[test]
    fn each_recorder_call_reaches_its_kafka_named_series() {
        let cases = [
            RecorderCase {
                name: "nothing recorded",
                record: |_| {},
                expected: EventQueueScrape::default(),
            },
            RecorderCase {
                name: "EventQueueTimeMs",
                record: |metrics| metrics.queue_time(Duration::from_millis(250)),
                expected: EventQueueScrape {
                    queue_count: 1.0,
                    queue_sum: 0.25,
                    ..EventQueueScrape::default()
                },
            },
            RecorderCase {
                name: "EventQueueProcessingTimeMs",
                record: |metrics| {
                    metrics.processing_time(Duration::from_millis(500));
                    metrics.processing_time(Duration::from_millis(250));
                },
                expected: EventQueueScrape {
                    processing_count: 2.0,
                    processing_sum: 0.75,
                    ..EventQueueScrape::default()
                },
            },
            RecorderCase {
                name: "EventQueueOperationsStartedCount and TimedOutCount",
                record: |metrics| {
                    metrics.operation_started();
                    metrics.operation_started();
                    metrics.operation_timed_out();
                },
                expected: EventQueueScrape {
                    started: 2.0,
                    timed_out: 1.0,
                    ..EventQueueScrape::default()
                },
            },
            RecorderCase {
                name: "AvgIdleRatio as a counter of idle seconds",
                record: |metrics| {
                    metrics.idle_time(Duration::from_millis(1_500));
                    metrics.idle_time(Duration::from_millis(500));
                },
                expected: EventQueueScrape {
                    idle_seconds: 2.0,
                    ..EventQueueScrape::default()
                },
            },
        ];
        for case in cases {
            let metrics = BrokerMetrics::new();
            (case.record)(&metrics);
            check!(
                EventQueueScrape::read(&metrics) == case.expected,
                "{}",
                case.name
            );
        }
    }
}
