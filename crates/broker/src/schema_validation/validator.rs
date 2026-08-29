//! The registry-backed record checker and its cache.
//!
//! The shape is the OPA authorizer's, for the same reasons: an LRU bounds what
//! a careless or hostile producer can make the broker hold, a TTL on each
//! entry is what makes a registry change observable, and failures are cached
//! as well as successes so that a produce storm against one bad id costs one
//! registry call rather than one per record.
//!
//! Not every failure is cached for the same length of time. "This id is not
//! registered" is an answer and keeps the full TTL; "the registry did not
//! answer" keeps [`UNAVAILABLE_TTL_MS`], because holding that one for minutes
//! would keep rejecting valid records after the registry recovered.
//!
//! It differs from the authorizer in one way that matters. `Authorizer` is a
//! synchronous trait, so `OpaAuthorizer` bridges to async with
//! `block_in_place`. This runs from `process_partition`, which is already
//! async, so a cache miss is a plain `.await` on one HTTP round trip and no
//! runtime worker is parked.

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use krabka_schema_serde::{
    error::SchemaSerdeError,
    format::validate::validate_body,
    registry::RegistryClient,
    subject::{Role, SchemaKind, SubjectStrategy as _, TopicNameStrategy},
    wire,
};
use krabka_units::{Time, convert::TimeExt as _};
use lru::LruCache;

use super::ValidationMode;
use crate::metrics::BrokerMetrics;

/// How long a "the registry could not answer" result stays cached.
///
/// Much shorter than `expire_after`, and deliberately so. A 404 is the
/// registry *answering*, so it earns the full TTL: it turns a produce storm
/// against one bad id into a single registry call. A timeout or a 5xx is the
/// registry *failing* to answer, and remembering that for minutes would keep
/// rejecting valid records long after the registry came back — the opposite of
/// what `fail_open = false` is for, which is to reject only while no answer is
/// available. Caching it briefly rather than not at all is what keeps an
/// outage from becoming one registry call per record.
const UNAVAILABLE_TTL_MS: i64 = 2_000;

/// Why a record failed validation.
///
/// Each variant is both a metric label, through [`RejectReason::label`], and
/// the KIP-467 `batch_index_error_message` the producer reads, through
/// [`std::fmt::Display`]. The two are deliberately different: the label has to
/// be low-cardinality for Prometheus, and the message has to name the id and
/// the subject so that a person can act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// The field carries no Confluent frame.
    Unframed(String),
    /// The frame is well formed but the registry does not know the id.
    UnknownId(u32),
    /// The id resolves, but it is not registered under this topic's subject.
    WrongSubject { id: u32, subject: String },
    /// The id resolves and belongs here, but the body is not an instance of
    /// the schema. Only [`ValidationMode::Full`] can produce this.
    BodyMismatch { id: u32, detail: String },
    /// The registry could not be reached or did not answer usefully, and
    /// `fail_open` is off.
    RegistryUnavailable(String),
}

impl RejectReason {
    /// The metric label for this reason. Low cardinality by construction: it
    /// carries none of the ids or subjects the message does.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Unframed(_) => "unframed",
            Self::UnknownId(_) => "unknown_id",
            Self::WrongSubject { .. } => "wrong_subject",
            Self::BodyMismatch { .. } => "body_mismatch",
            Self::RegistryUnavailable(_) => "registry_unavailable",
        }
    }
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unframed(detail) => {
                write!(f, "not a Confluent-framed payload: {detail}")
            }
            Self::UnknownId(id) => write!(f, "schema id {id} is not registered"),
            Self::WrongSubject { id, subject } => {
                write!(
                    f,
                    "schema id {id} is not registered under subject {subject}"
                )
            }
            Self::BodyMismatch { id, detail } => {
                write!(f, "body does not match schema id {id}: {detail}")
            }
            Self::RegistryUnavailable(detail) => {
                write!(f, "schema registry unavailable: {detail}")
            }
        }
    }
}

/// A [`SchemaValidator`] that could not be built from its configuration.
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidatorError {
    /// `maximum_cache_size` was zero, which would make every record a miss.
    #[error("schema_registry.maximum_cache_size must be greater than zero")]
    ZeroCache,
    /// The HTTP client could not be built from the configured timeout.
    #[error("schema registry HTTP client: {0}")]
    Http(String),
}

/// What the registry said about one schema id.
#[derive(Debug, Clone)]
struct SchemaEntry {
    /// Every subject this id is registered under. The subject check is a
    /// membership test against this set.
    subjects: HashSet<String>,
    /// The schema text and its format. Only [`ValidationMode::Full`] needs it,
    /// so it is fetched on the first `Full` check for this id and not before —
    /// an `Id`-mode topic never pays for the second registry call.
    body: Option<(SchemaKind, String)>,
}

/// One cached answer, positive or negative, with the instant it goes stale.
#[derive(Debug, Clone)]
struct Cached {
    /// `Err` is a negative cache entry: the id is not registered, or the
    /// registry could not say. Both are worth remembering for the TTL.
    entry: Result<SchemaEntry, RejectReason>,
    expires_at_ms: i64,
}

/// Registry-backed record validation with a bounded, expiring cache.
///
/// One instance per broker, held on [`crate::Broker`] as an `Option`. `None`
/// is "no `[schema_registry]` section", and then no topic can turn validation
/// on.
pub struct SchemaValidator {
    client: RegistryClient,
    cache: Mutex<LruCache<u32, Cached>>,
    expire_after: Time,
    /// **Security-sensitive.** `true` admits a record the broker could not
    /// validate because the registry was unreachable, which is fail-open: for
    /// the length of a registry outage, a validated topic accepts whatever it
    /// is sent. The default is `false`, which fails the produce instead. This
    /// is the same knob, with the same default and the same argument, as
    /// `allow_on_error` on [`crate::authorizer::opa::OpaAuthorizer`].
    ///
    /// An unknown id or a body that does not match its schema is a rejection
    /// under either setting. This governs only the case where the broker could
    /// not get an answer at all.
    fail_open: bool,
    /// Clock backing the cache TTL. Production uses
    /// [`qubit_clock::SystemClock`]; tests inject a `MockClock` so an expiry
    /// is an assertion rather than a sleep.
    clock: Arc<dyn qubit_clock::Clock>,
}

impl std::fmt::Debug for SchemaValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaValidator")
            .field("expire_after", &self.expire_after)
            .field("fail_open", &self.fail_open)
            .finish_non_exhaustive()
    }
}

impl SchemaValidator {
    /// Build a validator against the registry at `url`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidatorError::ZeroCache`] when `maximum_cache_size`
    /// is zero, and [`SchemaValidatorError::Http`] when the HTTP client cannot
    /// be built.
    pub fn new(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
    ) -> Result<Self, SchemaValidatorError> {
        Self::with_clock(
            url,
            fail_open,
            maximum_cache_size,
            expire_after,
            http_timeout,
            Arc::new(qubit_clock::SystemClock::new()),
        )
    }

    /// [`SchemaValidator::new`] with the clock injected, for tests that drive
    /// the cache TTL on a controlled timeline.
    ///
    /// # Errors
    ///
    /// As [`SchemaValidator::new`].
    pub fn with_clock(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        clock: Arc<dyn qubit_clock::Clock>,
    ) -> Result<Self, SchemaValidatorError> {
        let capacity =
            NonZeroUsize::new(maximum_cache_size).ok_or(SchemaValidatorError::ZeroCache)?;
        let http = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .build()
            .map_err(|e| SchemaValidatorError::Http(e.to_string()))?;
        Ok(Self {
            client: RegistryClient::with_http_client(url, http),
            cache: Mutex::new(LruCache::new(capacity)),
            expire_after,
            fail_open,
            clock,
        })
    }

    /// Check one record field.
    ///
    /// `field` is the record's key or value as it arrived. A null field is not
    /// passed here — the caller drops those, because a null value is a
    /// tombstone and a compacted topic needs it.
    ///
    /// `Ok(())` admits the record. `Err(reason)` rejects the whole batch with
    /// `INVALID_RECORD`, and `reason` becomes the producer's message.
    ///
    /// # Errors
    ///
    /// Returns the [`RejectReason`] that decided against the record.
    pub async fn check(
        &self,
        topic: &str,
        role: Role,
        mode: ValidationMode,
        field: &[u8],
        metrics: &BrokerMetrics,
    ) -> Result<(), RejectReason> {
        // A zero-length field is distinct from null on the wire, and some
        // clients write it for an absent value. It cannot carry a frame, and
        // rejecting it would reject those clients for a reason unrelated to
        // schemas.
        if field.is_empty() {
            return Ok(());
        }

        let (id, _) = wire::decode(field).map_err(|e| RejectReason::Unframed(e.to_string()))?;
        let subject = TopicNameStrategy.subject(topic, role);

        let entry = match self.entry(id, mode, metrics).await {
            Ok(entry) => entry,
            // A registry that could not answer is what `fail_open` governs. An
            // answer of "not registered" is an answer, and it rejects either
            // way.
            Err(reason) => return self.on_unavailable(reason),
        };
        if !entry.subjects.contains(&subject) {
            return Err(RejectReason::WrongSubject { id, subject });
        }

        if mode == ValidationMode::Id {
            return Ok(());
        }

        let Some((kind, text)) = entry.body.as_ref() else {
            // `entry` was fetched for `Full`, so the text is there unless the
            // registry answered without one. Treat that as unavailable rather
            // than as a passing record.
            return self.on_unavailable(RejectReason::RegistryUnavailable(format!(
                "registry returned no schema text for id {id}"
            )));
        };

        // Protobuf carries a message-index between the id and the body, so the
        // body offset depends on the format the id resolved to.
        let (message_index, body) = if *kind == SchemaKind::Protobuf {
            let (_, index, body) =
                wire::decode_protobuf(field).map_err(|e| RejectReason::Unframed(e.to_string()))?;
            (index, body)
        } else {
            let (_, body) =
                wire::decode(field).map_err(|e| RejectReason::Unframed(e.to_string()))?;
            (Vec::new(), body)
        };

        validate_body(*kind, text, &message_index, body).map_err(|e| RejectReason::BodyMismatch {
            id,
            detail: e.to_string(),
        })
    }

    /// The cached answer for `id`, fetching it when absent or stale.
    ///
    /// `mode` decides how much is fetched: `Full` also needs the schema text,
    /// and an entry cached by an earlier `Id` check does not carry it.
    async fn entry(
        &self,
        id: u32,
        mode: ValidationMode,
        metrics: &BrokerMetrics,
    ) -> Result<SchemaEntry, RejectReason> {
        let now = self.clock.millis();
        // Expired entries are not evicted eagerly; the read just declines
        // them. Lazy eviction is good enough at LRU capacities in the tens of
        // thousands, and it is what the OPA cache does.
        let hit = {
            let mut cache = self.cache.lock().expect("schema cache mutex poisoned");
            cache
                .get(&id)
                .filter(|cached| cached.expires_at_ms > now)
                .cloned()
        };
        if let Some(cached) = hit {
            match cached.entry {
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
        let fetched = self.fetch(id, mode).await;
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
            let mut cache = self.cache.lock().expect("schema cache mutex poisoned");
            cache.put(
                id,
                Cached {
                    entry: fetched.clone(),
                    expires_at_ms,
                },
            );
        }
        fetched
    }

    /// Ask the registry about `id`.
    async fn fetch(&self, id: u32, mode: ValidationMode) -> Result<SchemaEntry, RejectReason> {
        let bindings = self
            .client
            .subject_versions_for_id(id)
            .await
            .map_err(|e| Self::fetch_error(id, &e))?;
        let subjects = bindings.into_iter().map(|b| b.subject).collect();

        let body = if mode == ValidationMode::Full {
            let fetched = self
                .client
                .schema_by_id(id)
                .await
                .map_err(|e| Self::fetch_error(id, &e))?;
            Some((fetched.kind, fetched.schema))
        } else {
            None
        };

        Ok(SchemaEntry { subjects, body })
    }

    /// Turn a registry failure into the reason it stands for.
    ///
    /// A 404 is the registry answering: this id is not registered. Anything
    /// else is the registry failing to answer, which `fail_open` governs.
    fn fetch_error(id: u32, error: &SchemaSerdeError) -> RejectReason {
        match error {
            SchemaSerdeError::RegistryStatus { status: 404, .. } => RejectReason::UnknownId(id),
            other => RejectReason::RegistryUnavailable(other.to_string()),
        }
    }

    /// What an unreachable registry means, per `fail_open`.
    ///
    /// Fail-open covers only the case where the broker could not get an
    /// answer. "Not registered" is an answer, and it rejects under either
    /// setting.
    fn on_unavailable(&self, reason: RejectReason) -> Result<(), RejectReason> {
        if self.fail_open && matches!(reason, RejectReason::RegistryUnavailable(_)) {
            Ok(())
        } else {
            Err(reason)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use krabka_units::{millis, minutes, secs};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    const KNOWN_ID: u32 = 42;
    const AVRO: &str =
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;

    /// Frame a body the way a Confluent serializer does.
    fn framed(id: u32, body: &[u8]) -> Vec<u8> {
        let mut out = vec![0x00];
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// A registry that binds `KNOWN_ID` to `orders-value` and resolves it to
    /// [`AVRO`]. `expect` bounds how many times each endpoint may be called,
    /// which is how the cache tests assert a hit.
    async fn registry(versions_calls: u64) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])))
            .expect(versions_calls)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema": AVRO})),
            )
            .mount(&server)
            .await;
        server
    }

    fn validator(url: String) -> SchemaValidator {
        SchemaValidator::new(url, false, 100, minutes(1), secs(5)).expect("validator")
    }

    /// Metrics for a check whose counters the test does not assert on. The
    /// cache-accounting test binds one instance instead, so its hits and
    /// misses accumulate across calls.
    fn no_metrics() -> BrokerMetrics {
        BrokerMetrics::new()
    }

    /// [`UNAVAILABLE_TTL_MS`] as a `u64`, so a test advances the clock by the
    /// real constant rather than a copy of it that could drift.
    fn unavailable_ttl_ms() -> u64 {
        u64::try_from(UNAVAILABLE_TTL_MS).expect("the unavailable TTL is positive")
    }

    #[tokio::test]
    async fn a_field_that_carries_no_frame_is_rejected() {
        let server = registry(0).await;
        let v = validator(server.uri());
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("plain text", b"plain text, no serializer".to_vec()),
            ("bad magic", vec![0x01, 0, 0, 0, 42, b'x']),
            ("truncated id", vec![0x00, 0, 0]),
        ];
        for (name, field) in cases {
            let got = v
                .check(
                    "orders",
                    Role::Value,
                    ValidationMode::Id,
                    &field,
                    &no_metrics(),
                )
                .await;
            assert!(let Err(reason) = got, "case {name}");
            check!(reason.label() == "unframed", "case {name}: {reason}");
        }
    }

    #[tokio::test]
    async fn an_empty_field_is_accepted() {
        let server = registry(0).await;
        let v = validator(server.uri());
        // Distinct from null on the wire, and some clients write it for an
        // absent value. It cannot carry a frame, so rejecting it would reject
        // those clients for a reason unrelated to schemas.
        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &[],
                &no_metrics()
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn a_bound_id_is_accepted_and_an_unbound_one_is_not() {
        // `expect(1)`: the cache is keyed by schema id, so the second check
        // decides from the cached subject set without a second registry call.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");

        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok()
        );

        // Same id, different topic: the subject is `other-value`, which this
        // id is not registered under.
        let got = v
            .check(
                "other",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "wrong_subject", "{reason}");
    }

    #[tokio::test]
    async fn the_role_selects_the_subject() {
        // One call, for the same reason as above: the id is cached, and only
        // the subject the role derives differs between the two checks.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");

        // `orders-value` is bound; `orders-key` is not.
        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok()
        );
        let got = v
            .check(
                "orders",
                Role::Key,
                ValidationMode::Id,
                &field,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "wrong_subject", "{reason}");
    }

    #[tokio::test]
    async fn an_unregistered_id_is_rejected_and_the_rejection_is_cached() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99/versions"))
            .respond_with(ResponseTemplate::new(404))
            // One call for two checks: the negative answer is cached, so a
            // produce storm against one bad id costs one registry call.
            .expect(1)
            .mount(&server)
            .await;

        let v = validator(server.uri());
        let field = framed(99, b"anything");
        for _ in 0..2 {
            let got = v
                .check(
                    "orders",
                    Role::Value,
                    ValidationMode::Id,
                    &field,
                    &no_metrics(),
                )
                .await;
            assert!(let Err(reason) = got);
            check!(reason.label() == "unknown_id", "{reason}");
        }
    }

    #[tokio::test]
    async fn a_second_check_of_the_same_id_is_a_cache_hit() {
        // `expect(1)`: the mock fails the test if the second check calls it.
        let server = registry(1).await;
        let v = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        for _ in 0..3 {
            check!(
                v.check(
                    "orders",
                    Role::Value,
                    ValidationMode::Id,
                    &field,
                    &no_metrics()
                )
                .await
                .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn a_cache_entry_expires_on_its_ttl() {
        // Two calls: one before the TTL passes and one after.
        let server = registry(2).await;
        let clock = Arc::new(qubit_clock::MockClock::new());
        let v = SchemaValidator::with_clock(
            server.uri(),
            false,
            100,
            millis(10),
            secs(5),
            clock.clone(),
        )
        .expect("validator");
        let field = framed(KNOWN_ID, b"anything");

        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok()
        );
        // Past the TTL on a controlled timeline, so the expiry is an assertion
        // and not a race against a real sleep.
        clock.advance(Duration::from_millis(50));
        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn full_mode_checks_the_body_and_id_mode_does_not() {
        // Two calls to `/versions`. The `id` check caches an entry with no
        // schema text, because `id` mode never needs one; the `full` check
        // then has to complete that entry, and re-reads both endpoints. It
        // costs one extra GET per id per TTL, and only where one schema id is
        // used by both an `id`-mode and a `full`-mode topic.
        let server = registry(2).await;
        let v = validator(server.uri());
        // Framed with a bound id, but not an Avro datum of the schema it names.
        let field = framed(KNOWN_ID, &[0xFF; 6]);

        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok(),
            "id mode decides from the header alone"
        );
        let got = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &field,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "body_mismatch", "{reason}");
    }

    #[tokio::test]
    async fn full_mode_accepts_a_body_that_matches_its_schema() {
        let server = registry(1).await;
        let v = validator(server.uri());
        // One Avro datum of AVRO: `id = "a"`. A string is a zig-zag varint
        // length then the bytes, and 1 zig-zag encodes to 0x02.
        let field = framed(KNOWN_ID, &[0x02, b'a']);
        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &field,
                &no_metrics()
            )
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

        check!(
            v.check("orders", Role::Value, ValidationMode::Id, &field, &metrics)
                .await
                .is_ok()
        );
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 0);

        check!(
            v.check("orders", Role::Value, ValidationMode::Id, &field, &metrics)
                .await
                .is_ok()
        );
        check!(metrics.schema_validation_cache_misses.get() == 1);
        check!(metrics.schema_validation_cache_hits.get() == 1);
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
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])))
            .mount(&server)
            .await;

        let clock = Arc::new(qubit_clock::MockClock::new());
        let v = SchemaValidator::with_clock(
            server.uri(),
            false,
            100,
            minutes(5),
            secs(5),
            clock.clone(),
        )
        .expect("validator");
        let field = framed(KNOWN_ID, b"anything");

        let got = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "registry_unavailable", "{reason}");

        clock.advance(Duration::from_millis(unavailable_ttl_ms() + 1));
        check!(
            v.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok(),
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
        let clock = Arc::new(qubit_clock::MockClock::new());
        let v = SchemaValidator::with_clock(
            server.uri(),
            false,
            100,
            minutes(5),
            secs(5),
            clock.clone(),
        )
        .expect("validator");
        let field = framed(KNOWN_ID, b"anything");

        for _ in 0..2 {
            let got = v
                .check(
                    "orders",
                    Role::Value,
                    ValidationMode::Id,
                    &field,
                    &no_metrics(),
                )
                .await;
            assert!(let Err(reason) = got);
            check!(reason.label() == "unknown_id", "{reason}");
            clock.advance(Duration::from_millis(unavailable_ttl_ms() + 1));
        }
    }

    #[tokio::test]
    async fn an_unreachable_registry_fails_closed_or_open_by_the_knob() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let field = framed(KNOWN_ID, b"anything");

        let closed = SchemaValidator::new(server.uri(), false, 100, minutes(1), secs(5)).unwrap();
        let got = closed
            .check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "registry_unavailable", "{reason}");

        let open = SchemaValidator::new(server.uri(), true, 100, minutes(1), secs(5)).unwrap();
        check!(
            open.check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &field,
                &no_metrics()
            )
            .await
            .is_ok(),
            "fail_open admits what the registry could not answer for"
        );
    }

    #[tokio::test]
    async fn fail_open_does_not_admit_an_id_the_registry_says_is_unregistered() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        // 404 is the registry answering, not failing to answer. `fail_open`
        // governs only the second case.
        let v = SchemaValidator::new(server.uri(), true, 100, minutes(1), secs(5)).unwrap();
        let got = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &framed(KNOWN_ID, b"x"),
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "unknown_id", "{reason}");
    }

    #[test]
    fn a_zero_sized_cache_is_a_configuration_error() {
        let got = SchemaValidator::new(
            "http://localhost:8081".into(),
            false,
            0,
            minutes(1),
            secs(5),
        );
        assert!(let Err(SchemaValidatorError::ZeroCache) = got);
    }

    #[test]
    fn every_reject_reason_has_a_label_and_a_message() {
        let cases = [
            (RejectReason::Unframed("bad magic".into()), "unframed"),
            (RejectReason::UnknownId(7), "unknown_id"),
            (
                RejectReason::WrongSubject {
                    id: 7,
                    subject: "orders-value".into(),
                },
                "wrong_subject",
            ),
            (
                RejectReason::BodyMismatch {
                    id: 7,
                    detail: "nope".into(),
                },
                "body_mismatch",
            ),
            (
                RejectReason::RegistryUnavailable("timeout".into()),
                "registry_unavailable",
            ),
        ];
        for (reason, label) in cases {
            check!(reason.label() == label);
            check!(!reason.to_string().is_empty(), "{label}");
        }
    }
}
