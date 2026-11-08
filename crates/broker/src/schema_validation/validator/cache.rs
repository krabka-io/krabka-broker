//! The registry lookup and the bounded, expiring cache in front of it.
//!
//! This is where a schema id turns into an answer: a cache read, and on a miss
//! one or two registry calls whose result — positive or negative — is stored
//! with the instant it goes stale. The record check itself is in `check`, and
//! keeping the two apart means the caching policy, including the shorter TTL
//! for a registry that could not answer, reads in one place.

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use {
    super::{
        SchemaValidator, UNAVAILABLE_TTL_MS,
        compiled::CompiledSchema,
        references::{MAX_REFERENCE_BYTES, ReferenceSources, reference_sources_ordered},
        registry::RegistryReads,
        reject::RejectReason,
    },
    crate::{metrics::BrokerMetrics, schema_validation::ValidationMode, time_util},
    krabka_schema_serde::{error::SchemaSerdeError, subject::SchemaKind},
    krabka_units::convert::TimeExt as _,
    qubit_clock::WallClock as _,
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, OnceLock, PoisonError},
    },
    tokio::time,
};

/// What the registry said about one schema id.
///
/// Cached behind an [`Arc`], so a cache hit shares the entry rather than
/// copying its subject set once per record.
#[derive(Debug)]
pub(super) struct SchemaEntry {
    /// Every subject this id is registered under. The subject check is a
    /// membership test against this set.
    pub(super) subjects: HashSet<String>,
    /// The schema text and its format. Only [`ValidationMode::Full`] needs it,
    /// so it is fetched on the first `Full` check for this id and not before —
    /// an `Id`-mode topic never pays for the second registry call.
    pub(super) body: Option<Arc<SchemaBody>>,
}

/// A schema's text, its references and, once a `Full` check has needed it,
/// the compiled form of the three.
///
/// The text never changes for the life of the cache entry that holds it, so
/// the compiled form is built once, on first use, and shared by every later
/// check. It is dropped with the entry, so a TTL expiry or an LRU eviction
/// recompiles from the registry's fresh answer.
#[derive(derive_more::Debug)]
pub(super) struct SchemaBody {
    pub(super) kind: SchemaKind,
    pub(super) schema: String,
    pub(super) references: HashMap<String, String>,
    pub(super) reference_order: Vec<String>,
    #[debug(skip)]
    compiled: OnceLock<CompiledSchema>,
    /// How many times `compiled` was built. A test seam: the assertion that
    /// a schema is compiled once per entry and not once per record.
    #[cfg(test)]
    #[debug(skip)]
    compilations: AtomicUsize,
}

impl SchemaBody {
    pub(super) fn new(
        kind: SchemaKind,
        schema: String,
        references: HashMap<String, String>,
        reference_order: Vec<String>,
    ) -> Self {
        Self {
            kind,
            schema,
            references,
            reference_order,
            compiled: OnceLock::new(),
            #[cfg(test)]
            compilations: AtomicUsize::new(0),
        }
    }

    /// Check `body` against this schema, compiling it on the first call.
    ///
    /// # Errors
    ///
    /// As the per-format check: a [`SchemaSerdeError::Schema`] for a schema
    /// that does not compile, and a [`SchemaSerdeError::Deserialize`] for a
    /// body that is not an instance of it.
    pub(super) fn validate(
        &self,
        message_index: &[i32],
        body: &[u8],
    ) -> Result<(), SchemaSerdeError> {
        let compiled = self.compiled.get_or_init(|| {
            #[cfg(test)]
            self.compilations.fetch_add(1, Ordering::Relaxed);
            CompiledSchema::compile(
                self.kind,
                &self.schema,
                &self.references,
                &self.reference_order,
            )
        });
        compiled.validate(message_index, body)
    }

    #[cfg(test)]
    pub(super) fn compilations(&self) -> usize { self.compilations.load(Ordering::Relaxed) }
}

/// One cached answer, positive or negative, with the instant it goes stale.
#[derive(Debug, Clone)]
pub(super) struct Cached {
    /// `Err` is a negative cache entry: the id is not registered, or the
    /// registry could not say. Both are worth remembering for the TTL.
    entry: Result<Arc<SchemaEntry>, RejectReason>,
    expires_at_ms: i64,
}

impl SchemaValidator {
    /// The cached answer for `id`, fetching it when absent or stale.
    ///
    /// `mode` decides how much is fetched: `Full` also needs the schema text,
    /// and an entry cached by an earlier `Id` check does not carry it.
    pub(super) async fn entry(
        &self,
        id: u32,
        subject: &str,
        mode: ValidationMode,
        metrics: &BrokerMetrics,
    ) -> Result<Arc<SchemaEntry>, RejectReason> {
        self.entry_from(&self.client, id, subject, mode, metrics)
            .await
    }

    /// [`Self::entry`], reading a miss from `registry`.
    async fn entry_from(
        &self,
        registry: &impl RegistryReads,
        id: u32,
        subject: &str,
        mode: ValidationMode,
        metrics: &BrokerMetrics,
    ) -> Result<Arc<SchemaEntry>, RejectReason> {
        let now = time_util::epoch_millis(self.clock.now());
        // Expired entries are not evicted eagerly; the read just declines
        // them. Lazy eviction is good enough at LRU capacities in the tens of
        // thousands, and it is what the OPA cache does.
        let hit = {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache
                .get(&id)
                .filter(|cached| cached.expires_at_ms > now)
                .cloned()
        };
        if let Some(cached) = hit {
            match cached.entry {
                Ok(entry) if !entry.subjects.contains(subject) => {
                    metrics.record_schema_cache_hit();
                    return Ok(entry);
                }
                // A `Full` check needs the text; an entry without one was
                // cached by an `Id` check and has to be completed.
                Ok(entry) if mode == ValidationMode::Id || entry.body.is_some() => {
                    metrics.record_schema_cache_hit();
                    return Ok(entry);
                }
                // Cached by an `Id` check while this is `Full`: the text is
                // missing, so this costs a registry round trip like any other
                // miss and is counted as one.
                Ok(_) => {}
                Err(reason) => {
                    metrics.record_schema_cache_hit();
                    return Err(reason);
                }
            }
        }

        metrics.record_schema_cache_miss();
        let fetched = time::timeout(
            self.operation_timeout,
            Self::fetch(registry, id, subject, mode),
        )
        .await
        .unwrap_or_else(|_| {
            Err(RejectReason::RegistryUnavailable(format!(
                "schema registry operation exceeded {:?}",
                self.operation_timeout
            )))
        })
        .map(Arc::new);
        // A registry that could not answer is remembered only briefly, so the
        // next produce re-asks instead of inheriting a stale outage. Bounded by
        // `expire_after` so a shorter configured TTL still wins.
        let ttl_ms = if matches!(fetched, Err(RejectReason::RegistryUnavailable(_))) {
            UNAVAILABLE_TTL_MS.min(self.expire_after.millis_i64())
        } else {
            self.expire_after.millis_i64()
        };
        let expires_at_ms = now.saturating_add(ttl_ms);
        {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache.put(id, Cached {
                entry: fetched.clone(),
                expires_at_ms,
            });
        }
        fetched
    }

    /// Ask the registry about `id`.
    async fn fetch(
        registry: &impl RegistryReads,
        id: u32,
        subject: &str,
        mode: ValidationMode,
    ) -> Result<SchemaEntry, RejectReason> {
        let bindings = registry
            .subject_versions_for_id(id)
            .await
            .map_err(|e| Self::fetch_error(id, &e))?;
        let subjects: HashSet<_> = bindings.into_iter().map(|b| b.subject).collect();

        if !subjects.contains(subject) {
            return Ok(SchemaEntry {
                subjects,
                body: None,
            });
        }

        let body = if mode == ValidationMode::Full {
            let fetched = registry
                .schema_by_id(id)
                .await
                .map_err(|e| Self::fetch_error(id, &e))?;
            if fetched.schema.len() > MAX_REFERENCE_BYTES {
                return Err(RejectReason::RegistryRejected {
                    kind: krabka_verified::SchemaFailureKind::Permanent,
                    detail: format!("schema closure exceeds {MAX_REFERENCE_BYTES} bytes"),
                });
            }
            let ReferenceSources {
                sources: references,
                order: reference_order,
            } = reference_sources_ordered(
                registry,
                &fetched.references,
                MAX_REFERENCE_BYTES - fetched.schema.len(),
            )
            .await
            .map_err(|e| Self::fetch_error(id, &e))?;
            Some(Arc::new(SchemaBody::new(
                fetched.kind,
                fetched.schema,
                references,
                reference_order,
            )))
        } else {
            None
        };

        Ok(SchemaEntry { subjects, body })
    }

    /// Turn a registry failure into the reason it stands for.
    ///
    /// A 404 is an unknown ID. Transport errors, request timeouts, throttling,
    /// and 5xx responses are transient; other statuses and malformed success
    /// responses fail closed.
    fn fetch_error(id: u32, error: &SchemaSerdeError) -> RejectReason {
        use krabka_verified::SchemaFailureKind::{Malformed, Permanent};

        match error {
            SchemaSerdeError::RegistryStatus { status: 404, .. } => RejectReason::UnknownId(id),
            SchemaSerdeError::RegistryTransport(_)
            | SchemaSerdeError::RegistryStatus {
                status: 408 | 429 | 500..=599,
                ..
            } => RejectReason::RegistryUnavailable(error.to_string()),
            SchemaSerdeError::RegistryDecode(_) => RejectReason::RegistryRejected {
                kind: Malformed,
                detail: error.to_string(),
            },
            _ => RejectReason::RegistryRejected {
                kind: Permanent,
                detail: error.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::schema_validation::validator::test_support::{
            ClockedValidator, KNOWN_ID, clocked_validator, framed, no_metrics, registry,
            response_endpoint, unavailable_ttl_ms, validator, value_check,
        },
        assert2::{assert, check},
        krabka_schema_serde::{
            registry::{
                FetchedSchema,
                model::{SchemaReference, SubjectVersion},
            },
            subject::Role,
        },
        krabka_units::{millis, minutes},
        std::{sync::Mutex, time::Duration},
        wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        },
    };

    #[tokio::test]
    async fn an_unregistered_id_is_rejected_and_the_rejection_is_cached() {
        let server = MockServer::start().await;
        // One call for two checks: the negative answer is cached, so a
        // produce storm against one bad id costs one registry call.
        response_endpoint(
            &server,
            "/schemas/ids/99/versions",
            ResponseTemplate::new(404),
            Some(1),
        )
        .await;

        let v = validator(server.uri());
        let field = framed(99, b"anything");
        for run in [1, 2] {
            let err = value_check(&v, ValidationMode::Id, &field)
                .await
                .unwrap_err();
            check!(err.label() == "unknown_id", "run {run}: {err}");
        }
    }

    #[tokio::test]
    async fn a_second_check_of_the_same_id_is_a_cache_hit() {
        // `expect(1)`: the mock fails the test if the second check calls it.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        for _ in 0..3 {
            check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
        }
    }

    #[tokio::test]
    async fn a_cache_entry_expires_on_its_ttl() {
        // Two calls: one before the TTL passes and one after.
        let server = registry(2).await;
        let ClockedValidator {
            timeline,
            validator: v,
        } = clocked_validator(server.uri(), millis(10));
        let field = framed(KNOWN_ID, b"anything");

        check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
        // Past the TTL on a controlled timeline, so the expiry is an assertion
        // and not a race against a real sleep. The wall clock is anchored to
        // that timeline, so advancing it moves the cache's stamp with it.
        timeline
            .advance(Duration::from_millis(50))
            .expect("manual time moves forward");
        check!(value_check(&v, ValidationMode::Id, &field).await.is_ok());
    }

    async fn check_id(v: &SchemaValidator, field: &[u8], metrics: &BrokerMetrics) {
        check!(
            v.check("orders", Role::Value, ValidationMode::Id, field, metrics)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn the_cache_counters_move_on_a_miss_then_a_hit() {
        // `registry(1)` allows exactly one call to `/versions`, so the second
        // check is served from the cache. Without a counter on each path these
        // two gauges stay at zero for the life of the broker, which is what
        // this asserts against.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        let metrics = BrokerMetrics::new();

        check_id(&v, &field, &metrics).await;
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 0);

        check_id(&v, &field, &metrics).await;
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 1);
    }

    #[tokio::test]
    async fn a_full_cache_hit_shares_the_schema_closure() {
        let server = registry(1).await;
        let v = validator(server.uri());
        let metrics = no_metrics();
        let first = v
            .entry(KNOWN_ID, "orders-value", ValidationMode::Full, &metrics)
            .await
            .expect("first lookup")
            .body
            .clone()
            .expect("full lookup has a body");
        let second = v
            .entry(KNOWN_ID, "orders-value", ValidationMode::Full, &metrics)
            .await
            .expect("cached lookup")
            .body
            .clone()
            .expect("cached full lookup has a body");

        check!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn a_registry_that_could_not_answer_is_re_asked_after_the_short_ttl() {
        // The registry fails once and then recovers. A five-minute
        // `expire_after` must not keep rejecting for five minutes: a negative
        // entry for an unreachable registry carries its own short TTL, so the
        // next produce re-asks instead of inheriting the outage.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}/versions"),
            ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])),
            None,
        )
        .await;

        let ClockedValidator {
            timeline,
            validator: v,
        } = clocked_validator(server.uri(), minutes(5));
        let field = framed(KNOWN_ID, b"anything");

        let got = value_check(&v, ValidationMode::Id, &field).await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "registry_unavailable", "{reason}");

        timeline
            .advance(Duration::from_millis(unavailable_ttl_ms() + 1))
            .expect("manual time moves forward");
        check!(
            value_check(&v, ValidationMode::Id, &field).await.is_ok(),
            "the registry recovered, and the short negative TTL let the broker re-ask"
        );
    }

    #[tokio::test]
    async fn an_unregistered_id_stays_cached_past_the_unavailable_ttl() {
        // The short TTL is only for a registry that could not answer. A 404 is
        // the registry answering, so it keeps the full `expire_after` —
        // otherwise a produce storm against one bad id would re-ask every two
        // seconds. `expect(1)` is the assertion: the second check never asked.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let ClockedValidator {
            timeline,
            validator: v,
        } = clocked_validator(server.uri(), minutes(5));
        let field = framed(KNOWN_ID, b"anything");

        for _ in 0..2 {
            let got = value_check(&v, ValidationMode::Id, &field).await;
            assert!(let Err(reason) = got);
            check!(reason.label() == "unknown_id", "{reason}");
            timeline
                .advance(Duration::from_millis(unavailable_ttl_ms() + 1))
                .expect("manual time moves forward");
        }
    }

    /// A registry whose three reads each take a set time on tokio's clock, and
    /// which records which reads started, and when.
    ///
    /// Its latency is a tokio sleep and not a socket, so under paused time the
    /// whole lookup runs in virtual time and finishes the same way on any
    /// runner, however loaded.
    struct TimedRegistry {
        versions_latency: Duration,
        schema_latency: Duration,
        reference_latency: Duration,
        epoch: time::Instant,
        reads: Mutex<Vec<(&'static str, Duration)>>,
    }

    impl TimedRegistry {
        fn new(versions: Duration, schema: Duration, reference: Duration) -> Self {
            Self {
                versions_latency: versions,
                schema_latency: schema,
                reference_latency: reference,
                epoch: time::Instant::now(),
                reads: Mutex::new(Vec::new()),
            }
        }

        async fn read(&self, name: &'static str, latency: Duration) {
            self.reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((name, self.epoch.elapsed()));
            time::sleep(latency).await;
        }

        fn reads(&self) -> Vec<(&'static str, Duration)> {
            self.reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    impl RegistryReads for TimedRegistry {
        async fn subject_versions_for_id(
            &self,
            _id: u32,
        ) -> Result<Vec<SubjectVersion>, SchemaSerdeError> {
            self.read("subject_versions_for_id", self.versions_latency)
                .await;
            Ok(vec![SubjectVersion {
                subject: "orders-value".into(),
                version: 1,
            }])
        }

        async fn schema_by_id(&self, _id: u32) -> Result<FetchedSchema, SchemaSerdeError> {
            self.read("schema_by_id", self.schema_latency).await;
            Ok(FetchedSchema {
                schema: r#""string""#.into(),
                kind: SchemaKind::Avro,
                message_type: None,
                references: vec![SchemaReference {
                    name: "Leaf".into(),
                    subject: "leaf".into(),
                    version: 1,
                }],
            })
        }

        async fn schema_by_subject_version(
            &self,
            _subject: &str,
            _version: i32,
        ) -> Result<FetchedSchema, SchemaSerdeError> {
            self.read("schema_by_subject_version", self.reference_latency)
                .await;
            Ok(FetchedSchema {
                schema: r#""string""#.into(),
                kind: SchemaKind::Avro,
                message_type: None,
                references: Vec::new(),
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn one_deadline_bounds_the_whole_registry_operation() {
        // Three reads of 50 ms, 0 ms and 50 ms against an 80 ms deadline. Each
        // read fits inside the deadline, and so does each adjacent pair, but
        // the three together do not. Only a deadline over the whole operation
        // rejects it: not one per read, nor one per stage of the lookup.
        let registry = TimedRegistry::new(
            Duration::from_millis(50),
            Duration::ZERO,
            Duration::from_millis(50),
        );
        let v = SchemaValidator::new(
            "http://registry.invalid".into(),
            false,
            100,
            minutes(1),
            millis(80),
        )
        .expect("validator");

        let result = v
            .entry_from(
                &registry,
                KNOWN_ID,
                "orders-value",
                ValidationMode::Full,
                &no_metrics(),
            )
            .await;

        assert!(let Err(reason) = result);
        check!(
            reason
                == RejectReason::RegistryUnavailable(
                    "schema registry operation exceeded 80ms".into()
                )
        );
        // The third read started at 50 ms and was cut off at the deadline, 30
        // ms before it would have answered.
        check!(
            registry.reads()
                == [
                    ("subject_versions_for_id", Duration::ZERO),
                    ("schema_by_id", Duration::from_millis(50)),
                    ("schema_by_subject_version", Duration::from_millis(50)),
                ]
        );
        check!(registry.epoch.elapsed() == Duration::from_millis(80));
    }

    #[tokio::test]
    async fn a_registry_slower_than_the_deadline_is_unavailable() {
        // The same deadline over the real HTTP client. The registry answers
        // long after the 80 ms deadline, so either the operation deadline or
        // the request timeout ends the read, and both mean the registry could
        // not answer. No call count and no tight margin: nothing here races.
        let server = MockServer::start().await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}/versions"),
            ResponseTemplate::new(200).set_delay(Duration::from_secs(30)),
            None,
        )
        .await;
        let v = SchemaValidator::new(server.uri(), false, 100, minutes(1), millis(80))
            .expect("validator");

        let result = value_check(&v, ValidationMode::Id, &framed(KNOWN_ID, b"a")).await;

        assert!(let Err(reason) = result);
        check!(reason.label() == "registry_unavailable", "{reason}");
    }
}
