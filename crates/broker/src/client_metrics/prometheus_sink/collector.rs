//! The live snapshot itself: the keyed store of the newest point per series,
//! the ingest path that folds deltas into it, and the staleness bound that
//! decides which points a scrape still sees.
//!
//! Ingest is where the store is written and where stale entries are dropped,
//! so the mutex, the key shape, and the time-to-live sit here; `render` only
//! reads the same state at scrape time.

use {
    super::{DataPoint, PointValue},
    crate::metrics::{
        BrokerMetrics, ClientMetricsEviction, ClientMetricsEvictionLabel, ClientMetricsRejection,
        ClientMetricsRejectionLabel,
    },
    prometheus_client::metrics::{counter::Counter, family::Family, gauge::Gauge},
    std::{
        collections::HashMap,
        sync::{Mutex, PoisonError},
        time::{Duration, Instant},
    },
    uuid::Uuid,
};

/// The default retained-series ceilings. They count the series a scrape
/// renders, so a histogram counts each of its buckets plus `_sum` and
/// `_count`. `client_metrics_prom_max_series*` override them.
pub const MAX_RETAINED_SERIES: usize = 10_000;
pub const MAX_RETAINED_SERIES_PER_PRINCIPAL: usize = 2_500;
pub const MAX_RETAINED_SERIES_PER_CLIENT: usize = 1_000;
pub const MAX_RETAINED_SERIES_PER_INSTANCE: usize = 500;

/// Hard ceilings that keep the retained snapshot deterministic even when
/// clients reconnect with fresh instance ids or rotate their `client_id`.
/// They include updates to existing series; a point that would take any count
/// past its ceiling is discarded.
///
/// The principal ceiling is what a client cannot step around: the instance id
/// and the `client_id` are both the client's to choose, the authenticated
/// principal is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bon::Builder)]
pub struct SeriesCeilings {
    #[builder(default = MAX_RETAINED_SERIES)]
    pub(crate) total: usize,
    #[builder(default = MAX_RETAINED_SERIES_PER_PRINCIPAL)]
    pub(crate) per_principal: usize,
    #[builder(default = MAX_RETAINED_SERIES_PER_CLIENT)]
    pub(crate) per_client: usize,
    #[builder(default = MAX_RETAINED_SERIES_PER_INSTANCE)]
    pub(crate) per_instance: usize,
}

impl Default for SeriesCeilings {
    fn default() -> Self { Self::builder().build() }
}

/// The broker-registry handles the collector reports its own bookkeeping
/// into. Every count is in rendered series, the unit the ceilings use.
#[derive(Debug, Clone, Default)]
pub struct SeriesStats {
    pub(crate) accepted: Counter,
    pub(crate) rejected: Family<ClientMetricsRejectionLabel, Counter>,
    pub(crate) evicted: Family<ClientMetricsEvictionLabel, Counter>,
    pub(crate) retained: Gauge,
}

impl SeriesStats {
    /// The handles of `metrics`, so the collector updates the series the
    /// broker's `/metrics` route serves.
    pub(crate) fn of(metrics: &BrokerMetrics) -> Self {
        Self {
            accepted: metrics.client_metrics_series_accepted_total.clone(),
            rejected: metrics.client_metrics_series_rejected.clone(),
            evicted: metrics.client_metrics_series_evicted.clone(),
            retained: metrics.client_metrics_retained_series.clone(),
        }
    }

    pub(crate) fn reject(&self, reason: ClientMetricsRejection, series: usize) {
        self.rejected
            .get_or_create(&ClientMetricsRejectionLabel { reason })
            .inc_by(widen(series));
    }

    fn evict(&self, reason: ClientMetricsEviction, series: usize) {
        if series > 0 {
            self.evicted
                .get_or_create(&ClientMetricsEvictionLabel { reason })
                .inc_by(widen(series));
        }
    }

    fn publish_retained(&self, series: usize) {
        self.retained.set(i64::try_from(series).unwrap_or(i64::MAX));
    }
}

fn widen(series: usize) -> u64 { u64::try_from(series).unwrap_or(u64::MAX) }

#[derive(Debug)]
pub(super) struct StoredPoint {
    pub(super) attributes: Vec<(String, String)>,
    pub(super) value: PointValue,
    delta_start: Option<u64>,
    pub(super) at: Instant,
    /// The authenticated principal whose push last wrote the series, which
    /// the per-principal ceiling charges it to.
    principal: String,
}

/// The identity of one stored series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SeriesKey {
    pub(super) metric: String,
    pub(super) client_instance_id: String,
    pub(super) client_id: String,
    pub(super) attributes: Vec<(String, String)>,
}

/// The retained series counts that the ceilings bound, over the whole store
/// and by each key the store is bounded on.
#[derive(Debug, Default)]
struct Tally {
    total: usize,
    principals: HashMap<String, usize>,
    clients: HashMap<String, usize>,
    instances: HashMap<String, usize>,
}

impl Tally {
    fn of(points: &HashMap<SeriesKey, StoredPoint>) -> Self {
        let mut tally = Self::default();
        for (key, stored) in points {
            tally.add(&stored.principal, key, stored.value.series_count());
        }
        tally
    }

    fn add(&mut self, principal: &str, key: &SeriesKey, series: usize) {
        self.total += series;
        for count in [
            self.principals.entry(principal.to_owned()).or_default(),
            self.clients.entry(key.client_id.clone()).or_default(),
            self.instances
                .entry(key.client_instance_id.clone())
                .or_default(),
        ] {
            *count += series;
        }
    }

    fn remove(&mut self, principal: &str, key: &SeriesKey, series: usize) {
        self.total -= series;
        for count in [
            self.principals.get_mut(principal),
            self.clients.get_mut(&key.client_id),
            self.instances.get_mut(&key.client_instance_id),
        ]
        .into_iter()
        .flatten()
        {
            *count -= series;
        }
    }

    /// The first ceiling, narrowest first, that writing `after` series over
    /// `before` would breach. `principal_before` is the share of `before` that
    /// `principal` already owns. Only growth breaches: a write that keeps a
    /// count where it was or shrinks it is always admitted.
    fn breach(
        &self,
        ceilings: SeriesCeilings,
        principal: &str,
        key: &SeriesKey,
        (before, principal_before, after): (usize, usize, usize),
    ) -> Option<ClientMetricsRejection> {
        let count =
            |counts: &HashMap<String, usize>, name: &str| counts.get(name).copied().unwrap_or(0);
        [
            (
                ClientMetricsRejection::InstanceCeiling,
                count(&self.instances, &key.client_instance_id),
                before,
                ceilings.per_instance,
            ),
            (
                ClientMetricsRejection::ClientCeiling,
                count(&self.clients, &key.client_id),
                before,
                ceilings.per_client,
            ),
            (
                ClientMetricsRejection::PrincipalCeiling,
                count(&self.principals, principal),
                principal_before,
                ceilings.per_principal,
            ),
            (
                ClientMetricsRejection::GlobalCeiling,
                self.total,
                before,
                ceilings.total,
            ),
        ]
        .into_iter()
        .find(|&(_, count, prior, ceiling)| after > prior && count + after > ceiling + prior)
        .map(|(reason, ..)| reason)
    }
}

#[derive(Debug)]
pub struct ClientMetricsCollector {
    pub(super) points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    pub(super) ttl: Duration,
    ceilings: SeriesCeilings,
    stats: SeriesStats,
}

#[bon::bon]
impl ClientMetricsCollector {
    #[builder]
    pub(crate) fn new(
        ttl: Duration,
        #[builder(default)] ceilings: SeriesCeilings,
        #[builder(default)] stats: SeriesStats,
    ) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            ttl,
            ceilings,
            stats,
        }
    }
}

impl ClientMetricsCollector {
    pub(super) fn is_live(age: Duration, ttl: Duration) -> bool { age < ttl }

    /// The registry handles this collector reports into.
    pub(crate) fn stats(&self) -> &SeriesStats { &self.stats }

    /// Records the latest value for each point that `principal` pushed,
    /// replaces any earlier value, and removes stale points.
    pub(crate) fn ingest(&self, principal: &str, points: &[DataPoint]) {
        let now = Instant::now();
        let mut guard = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        self.evict_stale_locked(&mut guard, now);
        if self.ttl.is_zero() {
            return;
        }
        let mut tally = Tally::of(&guard);
        for p in points {
            let key = SeriesKey {
                metric: p.metric.clone(),
                client_instance_id: p.client_instance_id.clone(),
                client_id: p.client_id.clone(),
                attributes: p.attributes.clone(),
            };
            // A delta folds only into a total of its own shape and bucket
            // bounds, so the stored series renders as many lines afterwards as
            // the incoming point does.
            let after = p.value.series_count();
            let (before, owner) = guard.get(&key).map_or((0, None), |stored| {
                (stored.value.series_count(), Some(stored.principal.clone()))
            });
            let principal_before = if owner.as_deref() == Some(principal) {
                before
            } else {
                0
            };
            if let Some(reason) = tally.breach(
                self.ceilings,
                principal,
                &key,
                (before, principal_before, after),
            ) {
                self.stats.reject(reason, after);
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
                        principal.clone_into(&mut stored.principal);
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
                    principal: principal.to_owned(),
                });
            }
            if let Some(owner) = owner {
                tally.remove(&owner, &key, before);
            }
            tally.add(principal, &key, after);
            self.stats.accepted.inc_by(widen(after));
        }
        drop(guard);
        self.stats.publish_retained(tally.total);
    }

    /// Removes the series that outlived the TTL, counting them as `ttl`
    /// evictions. The broker's client-metrics eviction ticker calls it, so a
    /// stale series leaves the store even when no client pushes again.
    pub(crate) fn evict_stale(&self) {
        let mut guard = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        self.evict_stale_locked(&mut guard, Instant::now());
    }

    fn evict_stale_locked(&self, points: &mut HashMap<SeriesKey, StoredPoint>, now: Instant) {
        let mut evicted = 0;
        points.retain(|_, stored| {
            let live = Self::is_live(now.duration_since(stored.at), self.ttl);
            if !live {
                evicted += stored.value.series_count();
            }
            live
        });
        self.stats.evict(ClientMetricsEviction::Ttl, evicted);
        self.stats.publish_retained(retained(points));
    }

    /// Drops every retained series belonging to a disconnected client
    /// instance, preventing reconnect churn from occupying the cache until TTL.
    pub(crate) fn remove_instance(&self, instance: Uuid) {
        let instance = instance.to_string();
        let mut guard = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        let mut evicted = 0;
        guard.retain(|key, stored| {
            let keep = key.client_instance_id != instance;
            if !keep {
                evicted += stored.value.series_count();
            }
            keep
        });
        let retained_now = retained(&guard);
        drop(guard);
        self.stats.evict(ClientMetricsEviction::Disconnect, evicted);
        self.stats.publish_retained(retained_now);
    }

    /// The series a scrape would render now, stale ones included until the
    /// next eviction.
    #[cfg(test)]
    pub(crate) fn retained_series(&self) -> usize {
        retained(&self.points.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The count of points that are not stale. This method also removes the
    /// stale entries in place, counting them as evictions.
    #[cfg(test)]
    pub(crate) fn live_point_count(&self) -> usize {
        let mut guard = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        self.evict_stale_locked(&mut guard, Instant::now());
        guard.len()
    }
}

fn retained(points: &HashMap<SeriesKey, StoredPoint>) -> usize {
    points
        .values()
        .map(|stored| stored.value.series_count())
        .sum()
}

#[cfg(test)]
mod tests {
    use {
        super::{super::test_support::data_point, *},
        assert2::assert,
        std::{sync::Arc, thread, time::Duration},
    };

    const PRINCIPAL: &str = "User:telemetry";

    fn collector(ceilings: SeriesCeilings) -> ClientMetricsCollector {
        ClientMetricsCollector::builder()
            .ttl(Duration::from_mins(1))
            .ceilings(ceilings)
            .build()
    }

    fn gauge(metric: &str, instance: &str, client_id: &str) -> DataPoint {
        data_point()
            .metric(metric.into())
            .client_instance_id(instance.into())
            .client_id(client_id.into())
            .value(PointValue::Gauge(1.0))
            .call()
    }

    fn rejected(stats: &SeriesStats, reason: ClientMetricsRejection) -> u64 {
        stats
            .rejected
            .get_or_create(&ClientMetricsRejectionLabel { reason })
            .get()
    }

    fn evicted(stats: &SeriesStats, reason: ClientMetricsEviction) -> u64 {
        stats
            .evicted
            .get_or_create(&ClientMetricsEvictionLabel { reason })
            .get()
    }

    /// The retained series of `principal`.
    fn principal_series(sink: &ClientMetricsCollector, principal: &str) -> usize {
        Tally::of(&sink.points.lock().unwrap())
            .principals
            .get(principal)
            .copied()
            .unwrap_or(0)
    }

    /// Every series the store's tally counts, and every count it holds,
    /// within `ceilings`; and the gauge agrees with the store.
    fn assert_within(sink: &ClientMetricsCollector, ceilings: SeriesCeilings) {
        let tally = Tally::of(&sink.points.lock().unwrap());
        assert!(tally.total <= ceilings.total);
        assert!(
            tally
                .principals
                .values()
                .all(|&n| n <= ceilings.per_principal)
        );
        assert!(tally.clients.values().all(|&n| n <= ceilings.per_client));
        assert!(
            tally
                .instances
                .values()
                .all(|&n| n <= ceilings.per_instance)
        );
        assert!(sink.stats().retained.get() == i64::try_from(tally.total).unwrap());
    }

    #[test]
    fn attacker_controlled_series_are_bounded_and_disconnect_releases_them() {
        let sink = collector(SeriesCeilings::default());
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

        sink.ingest(PRINCIPAL, &points);
        assert!(sink.live_point_count() == MAX_RETAINED_SERIES_PER_INSTANCE);

        sink.remove_instance(instance);
        let stats = sink.stats();
        assert!(
            (
                sink.live_point_count(),
                stats.retained.get(),
                stats.accepted.get(),
                rejected(stats, ClientMetricsRejection::InstanceCeiling),
                evicted(stats, ClientMetricsEviction::Disconnect),
            ) == (0, 0, 500, 100, 500)
        );
    }

    #[test]
    fn a_histogram_counts_every_series_it_renders_toward_the_ceilings() {
        let sink = collector(SeriesCeilings::default());
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

        sink.ingest(PRINCIPAL, &points);
        assert!(
            (
                sink.live_point_count(),
                sink.retained_series(),
                rejected(sink.stats(), ClientMetricsRejection::InstanceCeiling),
            ) == (3, 393, 131)
        );
    }

    #[test]
    fn delta_points_accumulate_per_series() {
        let sink = collector(SeriesCeilings::default());
        for (counter, count, sum, buckets) in [
            (5.0, 2, 4.0, vec![(1.0, 1), (f64::MAX, 1)]),
            (3.0, 3, 6.0, vec![(1.0, 2), (f64::MAX, 1)]),
        ] {
            sink.ingest(PRINCIPAL, &[
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
                sink.ingest(PRINCIPAL, &[DataPoint {
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
            assert!(
                matches!(point.value, PointValue::Counter(value) if (value - expected).abs() < f64::EPSILON)
            );
            drop(guard);
        }

        let guard = sink.points.lock().unwrap();
        assert!(guard.values().any(
            |point| matches!(point.value, PointValue::Counter(value) if (value - 8.0).abs() < f64::EPSILON)
        ));
        assert!(guard.values().any(|point| matches!(
            &point.value,
            PointValue::Histogram { count: 5, sum, buckets }
                if (*sum - 10.0).abs() < f64::EPSILON
                    && buckets.as_slice() == [(1.0, 3), (f64::MAX, 2)]
        )));
        drop(guard);

        let mut total = PointValue::Histogram {
            count: 2,
            sum: 4.0,
            buckets: vec![(1.0, 1), (f64::MAX, 1)],
        };
        assert!(!total.accumulate(&PointValue::Histogram {
            count: 3,
            sum: 6.0,
            buckets: vec![(2.0, 2), (f64::MAX, 1)],
        }));
    }

    #[test]
    fn stale_points_evicted_on_encode() {
        let sink = ClientMetricsCollector::builder()
            .ttl(Duration::from_millis(0))
            .build();
        sink.ingest(PRINCIPAL, &[DataPoint {
            metric: "m".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            attributes: vec![],
            value: PointValue::Gauge(1.0),
            delta_start: None,
        }]);
        assert!((sink.live_point_count()) == (0));
        assert!(ClientMetricsCollector::is_live(
            Duration::from_nanos(9),
            Duration::from_nanos(10)
        ));
        assert!(!ClientMetricsCollector::is_live(
            Duration::from_nanos(10),
            Duration::from_nanos(10)
        ));
    }

    /// One push of the rows below: `count` gauges, each under a `client_id`
    /// and an instance id of its own, as a client that rotates both would
    /// send them.
    struct Push {
        principal: &'static str,
        count: usize,
    }

    /// `client_id` and the instance id are the client's to choose, so a
    /// principal that rotates both on every series is bounded only by the
    /// principal and global ceilings, and another principal keeps its share.
    #[test]
    fn rotating_client_ids_cannot_pass_the_principal_or_global_ceiling() {
        let ceilings = SeriesCeilings::builder()
            .total(100)
            .per_principal(40)
            .per_client(10)
            .per_instance(5)
            .build();
        for (
            name,
            pushes,
            expected_attacker,
            expected_victim,
            expected_principal,
            expected_global,
        ) in [
            (
                "one principal stops at its own ceiling",
                vec![Push {
                    principal: "User:attacker",
                    count: 500,
                }],
                40,
                0,
                460,
                0,
            ),
            (
                "a second principal is not starved",
                vec![
                    Push {
                        principal: "User:attacker",
                        count: 500,
                    },
                    Push {
                        principal: "User:victim",
                        count: 30,
                    },
                ],
                40,
                30,
                460,
                0,
            ),
            (
                "principals together stop at the global ceiling",
                vec![
                    Push {
                        principal: "User:attacker",
                        count: 40,
                    },
                    Push {
                        principal: "User:victim",
                        count: 40,
                    },
                    Push {
                        principal: "User:third",
                        count: 40,
                    },
                ],
                40,
                40,
                0,
                20,
            ),
        ] {
            let sink = collector(ceilings);
            for (batch, Push { principal, count }) in pushes.into_iter().enumerate() {
                let points = (0..count)
                    .map(|index| {
                        let id = format!("{batch}-{index}");
                        gauge("rotated", &id, &id)
                    })
                    .collect::<Vec<_>>();
                sink.ingest(principal, &points);
                assert_within(&sink, ceilings);
            }
            let stats = sink.stats();
            assert!(
                (
                    principal_series(&sink, "User:attacker"),
                    principal_series(&sink, "User:victim"),
                    rejected(stats, ClientMetricsRejection::PrincipalCeiling),
                    rejected(stats, ClientMetricsRejection::GlobalCeiling),
                    rejected(stats, ClientMetricsRejection::ClientCeiling)
                        + rejected(stats, ClientMetricsRejection::InstanceCeiling),
                ) == (
                    expected_attacker,
                    expected_victim,
                    expected_principal,
                    expected_global,
                    0
                ),
                "row {name}"
            );
        }
    }

    /// Every ceiling holds whichever key a push concentrates on, and a
    /// rejection counts against the narrowest ceiling it breaches.
    #[test]
    fn each_ceiling_bounds_its_own_key() {
        let ceilings = SeriesCeilings::builder()
            .total(30)
            .per_principal(20)
            .per_client(10)
            .per_instance(5)
            .build();
        for (name, instance_of, client_of, reason, expected_retained) in [
            (
                "one instance",
                (|_| "i".to_owned()) as fn(usize) -> String,
                (|_| "c".to_owned()) as fn(usize) -> String,
                ClientMetricsRejection::InstanceCeiling,
                5,
            ),
            (
                "one client across instances",
                |index| format!("i{index}"),
                |_| "c".to_owned(),
                ClientMetricsRejection::ClientCeiling,
                10,
            ),
            (
                "one principal across clients",
                |index| format!("i{index}"),
                |index| format!("c{index}"),
                ClientMetricsRejection::PrincipalCeiling,
                20,
            ),
        ] {
            let sink = collector(ceilings);
            let points = (0..50)
                .map(|index| gauge(&format!("m{index}"), &instance_of(index), &client_of(index)))
                .collect::<Vec<_>>();
            sink.ingest(PRINCIPAL, &points);
            assert_within(&sink, ceilings);
            let rejected_total: u64 = ClientMetricsRejection::ALL
                .into_iter()
                .map(|reason| rejected(sink.stats(), reason))
                .sum();
            assert!(
                (
                    sink.retained_series(),
                    rejected(sink.stats(), reason),
                    rejected_total
                ) == (
                    expected_retained,
                    50 - expected_retained as u64,
                    50 - expected_retained as u64
                ),
                "row {name}"
            );
        }
    }

    /// Updating a series this principal holds never counts as growth, so a
    /// client at its ceiling keeps reporting the series it already has.
    #[test]
    fn a_full_principal_still_updates_its_series() {
        let ceilings = SeriesCeilings::builder()
            .total(10)
            .per_principal(3)
            .per_client(3)
            .per_instance(3)
            .build();
        let sink = collector(ceilings);
        let points = (0..3)
            .map(|index| gauge(&format!("m{index}"), "i", "c"))
            .collect::<Vec<_>>();
        sink.ingest(PRINCIPAL, &points);
        sink.ingest(PRINCIPAL, &points);
        let stats = sink.stats();
        assert!((sink.retained_series(), stats.accepted.get()) == (3, 6));
        assert!(
            ClientMetricsRejection::ALL
                .into_iter()
                .all(|reason| rejected(stats, reason) == 0)
        );
    }

    #[test]
    fn concurrent_clients_stay_within_bounds() {
        let ceilings = SeriesCeilings::builder()
            .total(400)
            .per_principal(150)
            .per_client(60)
            .per_instance(25)
            .build();
        let sink = Arc::new(collector(ceilings));
        let handles = (0..16_u8)
            .map(|thread| {
                let sink = Arc::clone(&sink);
                thread::spawn(move || {
                    let principal = format!("User:p{}", thread % 4);
                    let instance = |round: u8| {
                        Uuid::from_u128((u128::from(thread) << 8) | u128::from(round % 7))
                    };
                    for round in 0..50_u8 {
                        let points = (0..20)
                            .map(|index| {
                                gauge(
                                    &format!("m{index}"),
                                    &instance(round).to_string(),
                                    &format!("c{thread}-{}", round % 3),
                                )
                            })
                            .collect::<Vec<_>>();
                        sink.ingest(&principal, &points);
                        if round % 10 == 9 {
                            sink.remove_instance(instance(round));
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("ingest thread");
        }
        assert_within(&sink, ceilings);
        assert!(sink.retained_series() > 0);
    }

    /// A series past its TTL leaves on the eviction tick and counts as a
    /// `ttl` eviction in rendered series, without another push.
    #[test]
    fn ttl_evictions_are_counted() {
        let ttl = Duration::from_mins(1);
        let sink = ClientMetricsCollector::builder().ttl(ttl).build();
        sink.ingest(PRINCIPAL, &[
            gauge("live", "i", "c"),
            gauge("stale", "i", "c"),
            data_point()
                .metric("stale_histogram".into())
                .value(PointValue::Histogram {
                    count: 2,
                    sum: 3.0,
                    buckets: vec![(1.0, 1), (f64::MAX, 1)],
                })
                .call(),
        ]);
        let aged = Instant::now()
            .checked_sub(ttl + Duration::from_secs(1))
            .expect("the clock is past one TTL");
        for (key, stored) in sink.points.lock().unwrap().iter_mut() {
            if key.metric.starts_with("stale") {
                stored.at = aged;
            }
        }
        assert!(sink.stats().retained.get() == 6);

        sink.evict_stale();

        let stats = sink.stats();
        assert!(
            (
                sink.retained_series(),
                stats.retained.get(),
                evicted(stats, ClientMetricsEviction::Ttl),
                evicted(stats, ClientMetricsEviction::Disconnect),
            ) == (1, 1, 5, 0)
        );
    }
}
