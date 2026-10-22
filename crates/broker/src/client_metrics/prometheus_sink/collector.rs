//! The live snapshot itself: the keyed store of the newest point per series,
//! the ingest path that folds deltas into it, and the staleness bound that
//! decides which points a scrape still sees.
//!
//! Ingest is where the store is written and where stale entries are dropped,
//! so the mutex, the key shape, and the time-to-live sit here; `render` only
//! reads the same state at scrape time.

use {
    super::{DataPoint, PointValue},
    std::{
        collections::HashMap,
        sync::Mutex,
        time::{Duration, Instant},
    },
    uuid::Uuid,
};

/// Hard ceilings keep the retained snapshot deterministic even when clients
/// reconnect with fresh instance ids. They count the series a scrape renders,
/// so a histogram counts each of its buckets plus `_sum` and `_count`. These
/// include existing series updates; a point that would take a total past
/// either ceiling is discarded.
pub(crate) const MAX_RETAINED_SERIES: usize = 10_000;
pub(crate) const MAX_RETAINED_SERIES_PER_CLIENT: usize = 1_000;
pub(crate) const MAX_RETAINED_SERIES_PER_INSTANCE: usize = 500;

#[derive(Debug)]
pub(super) struct StoredPoint {
    pub(super) attributes: Vec<(String, String)>,
    pub(super) value: PointValue,
    delta_start: Option<u64>,
    pub(super) at: Instant,
}

/// The identity of one stored series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SeriesKey {
    pub(super) metric: String,
    pub(super) client_instance_id: String,
    pub(super) client_id: String,
    pub(super) attributes: Vec<(String, String)>,
}

#[derive(Debug)]
pub(crate) struct ClientMetricsCollector {
    pub(super) points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    pub(super) ttl: Duration,
}

impl ClientMetricsCollector {
    pub(super) fn is_live(age: Duration, ttl: Duration) -> bool { age < ttl }

    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Records the latest value for each point, replaces any earlier value,
    /// and removes stale points.
    pub(crate) fn ingest(&self, points: &[DataPoint]) {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        if self.ttl.is_zero() {
            guard.clear();
            return;
        }
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
        let mut total = 0;
        let mut client_counts = HashMap::<String, usize>::new();
        let mut instance_counts = HashMap::<String, usize>::new();
        for (key, stored) in guard.iter() {
            let series = stored.value.series_count();
            total += series;
            *client_counts.entry(key.client_id.clone()).or_default() += series;
            *instance_counts
                .entry(key.client_instance_id.clone())
                .or_default() += series;
        }
        for p in points {
            let key = SeriesKey {
                metric: p.metric.clone(),
                client_instance_id: p.client_instance_id.clone(),
                client_id: p.client_id.clone(),
                attributes: p.attributes.clone(),
            };
            let before = guard
                .get(&key)
                .map_or(0, |stored| stored.value.series_count());
            let growth = p.value.series_count().saturating_sub(before);
            let client_count = client_counts.get(&key.client_id).copied().unwrap_or(0);
            let instance_count = instance_counts
                .get(&key.client_instance_id)
                .copied()
                .unwrap_or(0);
            if growth > 0
                && (total + growth > MAX_RETAINED_SERIES
                    || client_count + growth > MAX_RETAINED_SERIES_PER_CLIENT
                    || instance_count + growth > MAX_RETAINED_SERIES_PER_INSTANCE)
            {
                continue;
            }
            let accumulated = p.delta_start.is_some_and(|start| {
                guard.get_mut(&key).is_some_and(|stored| {
                    let accumulated = stored
                        .delta_start
                        .is_some_and(|previous| previous == 0 || start == 0 || previous == start)
                        && stored.value.accumulate(&p.value);
                    if accumulated {
                        stored.delta_start = Some(start);
                        stored.at = now;
                    }
                    accumulated
                })
            });
            if !accumulated {
                guard.insert(key.clone(), StoredPoint {
                    attributes: p.attributes.clone(),
                    value: p.value.clone(),
                    delta_start: p.delta_start,
                    at: now,
                });
            }
            let after = guard
                .get(&key)
                .map_or(0, |stored| stored.value.series_count());
            total = total + after - before;
            for count in [
                client_counts.entry(key.client_id.clone()).or_default(),
                instance_counts
                    .entry(key.client_instance_id.clone())
                    .or_default(),
            ] {
                *count = *count + after - before;
            }
        }
    }

    /// Drops every retained series belonging to a disconnected client
    /// instance, preventing reconnect churn from occupying the cache until TTL.
    pub(crate) fn remove_instance(&self, instance: Uuid) {
        let instance = instance.to_string();
        self.points
            .lock()
            .expect("prom sink mutex poisoned")
            .retain(|key, _| key.client_instance_id != instance);
    }

    /// The count of points that are not stale. This method also removes the
    /// stale entries in place.
    #[cfg(test)]
    pub(crate) fn live_point_count(&self) -> usize {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::test_support::data_point, *},
        std::time::Duration,
    };

    #[test]
    fn attacker_controlled_series_are_bounded_and_disconnect_releases_them() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        let instance = Uuid::from_u128(7);
        let points = (0..MAX_RETAINED_SERIES_PER_INSTANCE + 100)
            .map(|index| {
                data_point()
                    .metric(format!("attacker_metric_{index}"))
                    .client_instance_id(instance.to_string())
                    .client_id("attacker".into())
                    .value(PointValue::Gauge(f64::from(
                        u32::try_from(index).expect("index fits in u32"),
                    )))
                    .call()
            })
            .collect::<Vec<_>>();

        sink.ingest(&points);
        assert2::assert!(sink.live_point_count() == MAX_RETAINED_SERIES_PER_INSTANCE);

        sink.remove_instance(instance);
        assert2::assert!(sink.live_point_count() == 0);
    }

    #[test]
    fn a_histogram_counts_every_series_it_renders_toward_the_ceilings() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        let instance = Uuid::from_u128(8);
        // 129 buckets plus `_sum` and `_count` render 131 series, so three
        // histograms (393 series) fit under the per-instance ceiling of 500
        // and a fourth does not.
        let buckets = (0..129_u32)
            .map(|bound| (f64::from(bound), 1))
            .collect::<Vec<_>>();
        let points = (0..4)
            .map(|index| {
                data_point()
                    .metric(format!("attacker_histogram_{index}"))
                    .client_instance_id(instance.to_string())
                    .client_id("attacker".into())
                    .value(PointValue::Histogram {
                        count: 129,
                        sum: 1.0,
                        buckets: buckets.clone(),
                    })
                    .call()
            })
            .collect::<Vec<_>>();

        sink.ingest(&points);
        assert2::assert!(sink.live_point_count() == 3);
    }

    #[test]
    fn delta_points_accumulate_per_series() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        for (counter, count, sum, buckets) in [
            (5.0, 2, 4.0, vec![(1.0, 1), (f64::MAX, 1)]),
            (3.0, 3, 6.0, vec![(1.0, 2), (f64::MAX, 1)]),
        ] {
            sink.ingest(&[
                data_point()
                    .metric("requests".into())
                    .value(PointValue::Counter(counter))
                    .delta_start(7)
                    .call(),
                data_point()
                    .metric("latency".into())
                    .value(PointValue::Histogram {
                        count,
                        sum,
                        buckets,
                    })
                    .delta_start(7)
                    .call(),
            ]);
        }
        for (metric, first_start, second_start, expected) in [
            ("unknown_previous", 0, 8, 8.0),
            ("unknown_current", 8, 0, 8.0),
            ("reset", 7, 8, 3.0),
        ] {
            for (value, start) in [(5.0, first_start), (3.0, second_start)] {
                sink.ingest(&[DataPoint {
                    metric: metric.into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                    value: PointValue::Counter(value),
                    delta_start: Some(start),
                }]);
            }
            let guard = sink.points.lock().unwrap();
            let point = guard
                .get(&SeriesKey {
                    metric: metric.into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                })
                .unwrap();
            assert2::assert!(
                matches!(point.value, PointValue::Counter(value) if (value - expected).abs() < f64::EPSILON)
            );
        }

        let guard = sink.points.lock().unwrap();
        assert2::assert!(guard.values().any(
            |point| matches!(point.value, PointValue::Counter(value) if (value - 8.0).abs() < f64::EPSILON)
        ));
        assert2::assert!(guard.values().any(|point| matches!(
            &point.value,
            PointValue::Histogram { count: 5, sum, buckets }
                if (*sum - 10.0).abs() < f64::EPSILON
                    && buckets.as_slice() == [(1.0, 3), (f64::MAX, 2)]
        )));

        let mut total = PointValue::Histogram {
            count: 2,
            sum: 4.0,
            buckets: vec![(1.0, 1), (f64::MAX, 1)],
        };
        assert2::assert!(!total.accumulate(&PointValue::Histogram {
            count: 3,
            sum: 6.0,
            buckets: vec![(2.0, 2), (f64::MAX, 1)],
        }));
    }

    #[test]
    fn stale_points_evicted_on_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_millis(0));
        sink.ingest(&[DataPoint {
            metric: "m".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            attributes: vec![],
            value: PointValue::Gauge(1.0),
            delta_start: None,
        }]);
        assert2::assert!((sink.live_point_count()) == (0));
        assert2::assert!(ClientMetricsCollector::is_live(
            Duration::from_nanos(9),
            Duration::from_nanos(10)
        ));
        assert2::assert!(!ClientMetricsCollector::is_live(
            Duration::from_nanos(10),
            Duration::from_nanos(10)
        ));
    }
}
