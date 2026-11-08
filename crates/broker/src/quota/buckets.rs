//! Per-broker cache of quota meters: one [`ClientQuota`] per
//! (`quota_type`, `entity_key`) pair, for the KIP-13 and KIP-124 client quotas
//! and the KIP-612 `connection_creation_rate` of each `ip` entity.

use {
    super::{IpNames, QuotaType, entity_field},
    crate::throttle::{ClientQuota, QuotaWindow},
    dashmap::DashMap,
    krabka_metadata::EntityKey,
    qubit_clock::{MonotonicClock, MonotonicInstant, StdMonotonicClock},
    std::{
        hash::Hash,
        sync::{Arc, Mutex, PoisonError},
        time::{Duration, Instant},
    },
};

/// A stored quota meter and when a request last used it. It holds no client
/// identity: the meter belongs to its entity key, and every client that
/// resolves to the key shares it, so the first one to create it must not stand
/// for the rest (#1213).
#[derive(Debug)]
pub struct MeterEntry<M> {
    pub meter: Arc<M>,
    last_accessed: Mutex<MonotonicInstant>,
}

impl<M> MeterEntry<M> {
    fn new(meter: Arc<M>, now: MonotonicInstant) -> Self {
        Self {
            meter,
            last_accessed: Mutex::new(now),
        }
    }

    fn touch(&self, now: MonotonicInstant) {
        *self
            .last_accessed
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = now;
    }

    /// Whether nothing used the meter for longer than `max_age` by `now`.
    fn idle_longer_than(&self, now: MonotonicInstant, max_age: Duration) -> bool {
        let last = *self
            .last_accessed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        now.duration_since(last).is_ok_and(|idle| idle > max_age)
    }
}

/// Where a meter is stored: its quota type and its canonical entity key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketKey {
    pub quota_type: QuotaType,
    pub entity_key: EntityKey,
}

/// A meter that [`QuotaBuckets::expire_inactive`] dropped, with the labels the
/// metric series it carried were published under. The user and the client id
/// are the ones of the meter's entity key, which are the labels a throttle
/// charged to it published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredBucket {
    pub quota_type: QuotaType,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

/// A live meter and the key it is stored under.
#[derive(Debug)]
pub struct LiveMeter<M> {
    pub key: BucketKey,
    pub entry: Arc<MeterEntry<M>>,
}

#[derive(derive_more::Debug)]
pub struct QuotaBuckets {
    /// The `producer_byte_rate`, `consumer_byte_rate` and
    /// `request_percentage` quota of each entity, and the
    /// `connection_creation_rate` of each `ip` entity, created on first use as
    /// Kafka's `ClientQuotaManager.getOrCreateQuotaSensors` and
    /// `ConnectionQuotas.getOrCreateConnectionRateQuotaSensor` create the
    /// entity's sensor.
    client_quotas: DashMap<BucketKey, Arc<MeterEntry<ClientQuota>>>,
    controller_mutations: DashMap<EntityKey, Arc<Mutex<ControllerMutationBucket>>>,
    /// The sample window of every client quota.
    client_quota_window: QuotaWindow,
    /// The clock the client quotas measure on and idle meters expire by.
    #[debug(skip)]
    clock: Arc<dyn MonotonicClock>,
    /// The addresses the image's `ip` quota entity names stand for.
    ip_names: IpNames,
}

impl Default for QuotaBuckets {
    fn default() -> Self { Self::new() }
}

#[derive(Debug)]
pub(super) struct ControllerMutationBucket {
    pub(super) rate: f64,
    pub(super) window_secs: f64,
    pub(super) tokens: f64,
    pub(super) updated_at: Instant,
}

#[bon::bon]
impl QuotaBuckets {
    /// Meters over Kafka's default windows, on the system's monotonic clock.
    /// The broker builds its own with [`Self::configured`] from its config;
    /// this is for callers that have no config to read, which is the
    /// in-process tests.
    #[must_use]
    pub fn new() -> Self { Self::configured().call() }

    /// Meters whose client quotas sample over `client_quota_window`, Kafka's
    /// `quota.window.num` samples of `quota.window.size.seconds`. Tests pass a [`qubit_clock::ManualMonotonicClock`] as `clock`.
    #[builder]
    pub fn configured(
        #[builder(default)] client_quota_window: QuotaWindow,
        #[builder(default = Arc::new(StdMonotonicClock::new()))] clock: Arc<dyn MonotonicClock>,
    ) -> Self {
        Self {
            client_quotas: DashMap::new(),
            controller_mutations: DashMap::new(),
            client_quota_window,
            clock,
            ip_names: IpNames::default(),
        }
    }
}

impl QuotaBuckets {
    /// The sample window every client quota measures over.
    #[must_use]
    pub fn client_quota_window(&self) -> QuotaWindow { self.client_quota_window }

    /// The addresses the image's `ip` quota entity names stand for, which the
    /// quota refresh task keeps current.
    #[must_use]
    pub fn ip_names(&self) -> &IpNames { &self.ip_names }

    /// Returns the client quota for `(quota_type, entity_key)`, and creates it
    /// on first use, bounded at `bound` per second in the unit the quota
    /// records. `quota_type` is `Produce`, `Fetch`, `Request` or
    /// `ConnectionCreation`; a `ConnectionCreation` key is an `ip` entity.
    #[must_use]
    pub fn client_quota(
        &self,
        quota_type: QuotaType,
        entity_key: &EntityKey,
        bound: f64,
    ) -> Arc<ClientQuota> {
        let key = BucketKey {
            quota_type,
            entity_key: entity_key.clone(),
        };
        get_or_insert(&self.client_quotas, key, self.clock.now(), || {
            ClientQuota::with_clock(Arc::clone(&self.clock), self.client_quota_window, bound)
        })
    }

    /// Iterates over every live client quota with the key it is stored under.
    /// The refresh task uses it to push new bounds after an image change.
    pub fn client_quotas(&self) -> impl Iterator<Item = LiveMeter<ClientQuota>> + '_ {
        self.client_quotas.iter().map(|r| LiveMeter {
            key: r.key().clone(),
            entry: Arc::clone(r.value()),
        })
    }

    /// Expire meters unused for more than `max_age` (Kafka uses 1 hour).
    ///
    /// Returns every meter it dropped, so the caller can unregister the metric
    /// series they carried.
    #[must_use]
    pub fn expire_inactive(&self, max_age: Duration) -> Vec<ExpiredBucket> {
        let now = self.clock.now();
        let mut expired = Vec::new();
        retain_active(&self.client_quotas, now, max_age, |key| {
            expired.push(expired_bucket(key.quota_type, &key.entity_key));
        });
        expired
    }

    pub(super) fn controller_mutation_bucket(
        &self,
        entity_key: &EntityKey,
        rate: f64,
        window_secs: f64,
    ) -> Arc<Mutex<ControllerMutationBucket>> {
        self.controller_mutations
            .entry(entity_key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(ControllerMutationBucket {
                    rate,
                    window_secs,
                    tokens: rate * window_secs,
                    updated_at: Instant::now(),
                }))
            })
            .clone()
    }

    #[cfg(test)]
    #[must_use]
    pub fn len(&self) -> usize { self.client_quotas.len() }

    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// The meter stored under `key`, marked used at `now`, or a new one from
/// `create` when there is none.
fn get_or_insert<K: Eq + Hash, M>(
    meters: &DashMap<K, Arc<MeterEntry<M>>>,
    key: K,
    now: MonotonicInstant,
    create: impl FnOnce() -> M,
) -> Arc<M> {
    if let Some(entry) = meters.get(&key) {
        entry.touch(now);
        return Arc::clone(&entry.meter);
    }
    let entry = meters
        .entry(key)
        .or_insert_with(|| Arc::new(MeterEntry::new(Arc::new(create()), now)));
    entry.touch(now);
    Arc::clone(&entry.meter)
}

/// Drops every meter idle longer than `max_age` by `now`, and hands each
/// dropped key to `dropped`.
fn retain_active<K: Eq + Hash, M>(
    meters: &DashMap<K, Arc<MeterEntry<M>>>,
    now: MonotonicInstant,
    max_age: Duration,
    mut dropped: impl FnMut(&K),
) {
    meters.retain(|key, entry| {
        let idle = entry.idle_longer_than(now, max_age);
        if idle {
            dropped(key);
        }
        !idle
    });
}

fn expired_bucket(quota_type: QuotaType, entity_key: &EntityKey) -> ExpiredBucket {
    ExpiredBucket {
        quota_type,
        user: entity_field(entity_key, "user"),
        client_id: entity_field(entity_key, "client-id"),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        krabka_units::{millis, secs},
        qubit_clock::ManualMonotonicClock,
        std::{iter, num::NonZeroU32},
    };

    fn key(user: &str) -> EntityKey { vec![("user".into(), Some(user.into()))] }

    /// Meters on a manual clock over `window`.
    fn manual(window: QuotaWindow) -> (Arc<ManualMonotonicClock>, QuotaBuckets) {
        let clock = ManualMonotonicClock::new_shared();
        let buckets = QuotaBuckets::configured()
            .client_quota_window(window)
            .clock(Arc::<ManualMonotonicClock>::clone(&clock))
            .call();
        (clock, buckets)
    }

    /// KIP-13 measures a client's rate over `quota.window.num` samples of
    /// `quota.window.size.seconds`, and Kafka's `Rate` pads a new client's
    /// window to `quota.window.num - 1` samples, so it may send that many
    /// samples' worth at once before it is throttled: 10 240 bytes at
    /// 1024 B/s over 11 samples of 1 s, and 1024 over 2 samples. One byte
    /// more is over the bound (#397, #418).
    #[yare::parameterized(
        eleven_samples = { 11, 10_240 },
        two_samples = { 2, 1_024 },
    )]
    fn the_configured_window_sizes_a_new_client_s_burst(samples: u32, within: u64) {
        let window = QuotaWindow::new(NonZeroU32::new(samples).unwrap(), secs(1));
        let (_, buckets) = manual(window);
        let quota = buckets.client_quota(QuotaType::Produce, &key("alice"), 1_024.0);

        check!(quota.record(within) == millis(0));
        check!(
            quota.record(1) > millis(0),
            "{window:?} burst was not exhausted"
        );
    }

    #[test]
    fn client_quota_is_created_at_its_bound_the_first_time() {
        let buckets = QuotaBuckets::new();
        let quota = buckets.client_quota(QuotaType::Produce, &key("alice"), 1024.0);
        check!((quota.bound() - 1024.0).abs() < 1.0e-9);
        assert!(buckets.len() == 1);
    }

    #[test]
    fn client_quota_returns_the_existing_quota_the_second_time() {
        let buckets = QuotaBuckets::new();
        let first = buckets.client_quota(QuotaType::Produce, &key("alice"), 1024.0);
        let second = buckets.client_quota(QuotaType::Produce, &key("alice"), 4096.0);
        // Same Arc: the bound on the second call is ignored.
        check!(Arc::ptr_eq(&first, &second));
        check!((first.bound() - 1024.0).abs() < 1.0e-9);
        check!(buckets.len() == 1);
    }

    #[test]
    fn different_quota_types_get_different_meters() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.client_quota(QuotaType::Produce, &key("alice"), 1024.0);
        let _ = buckets.client_quota(QuotaType::Fetch, &key("alice"), 2048.0);
        let _ = buckets.client_quota(QuotaType::ConnectionCreation, &key("alice"), 2.0);
        assert!(buckets.len() == 3);
    }

    #[test]
    fn different_entities_get_different_meters() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.client_quota(QuotaType::Produce, &key("alice"), 1024.0);
        let _ = buckets.client_quota(QuotaType::Produce, &key("bob"), 2048.0);
        assert!(buckets.len() == 2);
    }

    /// A `connection_creation_rate` is measured like any other client quota:
    /// a new `ip` entity at 2 connections a second may open 20 at once over
    /// the ten padded samples, and the 21st is 0.1 a second over, a 500 ms
    /// throttle.
    #[test]
    fn a_connection_rate_admits_its_padded_window_at_once() {
        let (_, buckets) = manual(QuotaWindow::KAFKA_DEFAULT);
        let quota = buckets.client_quota(QuotaType::ConnectionCreation, &key("alice"), 2.0);

        let throttles: Vec<_> = iter::repeat_with(|| quota.record(1)).take(21).collect();
        let expected: Vec<_> = iter::repeat_n(millis(0), 20).chain([millis(500)]).collect();
        check!(throttles == expected);
    }

    /// A meter expires once it has been idle longer than the age, on the
    /// meters' own clock, and using it keeps it alive.
    #[test]
    fn an_idle_meter_expires_and_a_used_one_does_not() {
        let (clock, buckets) = manual(QuotaWindow::KAFKA_DEFAULT);
        let _ = buckets.client_quota(QuotaType::Produce, &key("alice"), 1.0);
        let _ = buckets.client_quota(QuotaType::Produce, &key("bob"), 1.0);
        let _ = buckets.client_quota(QuotaType::ConnectionCreation, &key("carol"), 1.0);

        check!(let Ok(()) = clock.advance(Duration::from_secs(10)));
        let _ = buckets.client_quota(QuotaType::Produce, &key("bob"), 1.0);
        check!(let Ok(()) = clock.advance(Duration::from_secs(1)));
        let mut expired = buckets.expire_inactive(Duration::from_secs(10));
        expired.sort_by(|a, b| a.user.cmp(&b.user));

        check!(
            expired
                == vec![
                    ExpiredBucket {
                        quota_type: QuotaType::Produce,
                        user: Some("alice".into()),
                        client_id: None,
                    },
                    ExpiredBucket {
                        quota_type: QuotaType::ConnectionCreation,
                        user: Some("carol".into()),
                        client_id: None,
                    },
                ]
        );
        check!(buckets.len() == 1);
    }
}
