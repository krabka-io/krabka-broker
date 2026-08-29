//! The registry lookup and the bounded, expiring cache in front of it.
//!
//! This is where a schema id turns into an answer: a cache read, and on a miss
//! one or two registry calls whose result — positive or negative — is stored
//! with the instant it goes stale. The record check itself is in `check`, and
//! keeping the two apart means the caching policy, including the shorter TTL
//! for a registry that could not answer, reads in one place.

use std::collections::HashSet;

use krabka_schema_serde::{error::SchemaSerdeError, subject::SchemaKind};
use krabka_units::convert::TimeExt as _;

use super::{SchemaValidator, UNAVAILABLE_TTL_MS, reject::RejectReason};
use crate::{metrics::BrokerMetrics, schema_validation::ValidationMode};

/// What the registry said about one schema id.
#[derive(Debug, Clone)]
pub(super) struct SchemaEntry {
    /// Every subject this id is registered under. The subject check is a
    /// membership test against this set.
    pub(super) subjects: HashSet<String>,
    /// The schema text and its format. Only [`ValidationMode::Full`] needs it,
    /// so it is fetched on the first `Full` check for this id and not before —
    /// an `Id`-mode topic never pays for the second registry call.
    pub(super) body: Option<(SchemaKind, String)>,
}

/// One cached answer, positive or negative, with the instant it goes stale.
#[derive(Debug, Clone)]
pub(super) struct Cached {
    /// `Err` is a negative cache entry: the id is not registered, or the
    /// registry could not say. Both are worth remembering for the TTL.
    entry: Result<SchemaEntry, RejectReason>,
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
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_schema_serde::subject::Role;
    use krabka_units::{millis, minutes, secs};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::schema_validation::validator::test_support::{
        KNOWN_ID, framed, no_metrics, registry, unavailable_ttl_ms, validator,
    };

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
}
