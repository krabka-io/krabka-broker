//! The per-record check: the Confluent frame, the subject binding, and — in
//! full mode — the body decode against the schema the frame names.
//!
//! The body step is where the format matters, because Protobuf puts a
//! message-index between the id and the payload and Avro and JSON Schema do
//! not. That per-format detail, and the `fail_open` decision that a registry
//! failure runs into, are what this module holds; the lookup behind it is in
//! `cache`.

use {
    super::{SchemaValidator, reject::RejectReason},
    crate::{metrics::BrokerMetrics, schema_validation::ValidationMode},
    krabka_schema_serde::{
        subject::{Role, SchemaKind, SubjectStrategy as _, TopicNameStrategy},
        wire,
    },
};

impl SchemaValidator {
    /// The registry subject a `role` field of `topic` must be registered
    /// under.
    ///
    /// It depends only on the topic and the role, so a caller checking a
    /// whole batch derives it once per role and passes it to
    /// [`SchemaValidator::check_subject`] for every record.
    #[must_use]
    pub fn subject(&self, topic: &str, role: Role) -> String {
        TopicNameStrategy.subject(topic, role)
    }

    /// Check one record field of `topic`, deriving its subject from `role`.
    ///
    /// # Errors
    ///
    /// As [`SchemaValidator::check_subject`].
    pub async fn check(
        &self,
        topic: &str,
        role: Role,
        mode: ValidationMode,
        field: &[u8],
        metrics: &BrokerMetrics,
    ) -> Result<(), RejectReason> {
        self.check_subject(&self.subject(topic, role), mode, field, metrics)
            .await
    }

    /// Check one record field against `subject`, as
    /// [`SchemaValidator::subject`] derived it.
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
    pub async fn check_subject(
        &self,
        subject: &str,
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

        let [magic, id_0, id_1, id_2, id_3, after_prefix @ ..] = field else {
            return Err(RejectReason::Unframed(
                "Confluent frame is shorter than its five-byte prefix".to_owned(),
            ));
        };
        let Some(id) = krabka_verified::schema_frame_id(*magic, *id_0, *id_1, *id_2, *id_3) else {
            return Err(RejectReason::Unframed(format!(
                "unsupported Confluent magic byte {magic}"
            )));
        };
        let entry = match self.entry(id, subject, mode, metrics).await {
            Ok(entry) => entry,
            // A registry that could not answer is what `fail_open` governs. An
            // answer of "not registered" is an answer, and it rejects either
            // way.
            Err(reason) => return self.on_unavailable(reason),
        };
        if !entry.subjects.contains(subject) {
            return Err(RejectReason::WrongSubject {
                id,
                subject: subject.to_owned(),
            });
        }

        if mode == ValidationMode::Id {
            return Ok(());
        }

        let Some(schema_body) = entry.body.as_ref() else {
            // `entry` was fetched for `Full`, so the text is there unless the
            // registry answered without one. Treat that as unavailable rather
            // than as a passing record.
            return self.on_unavailable(RejectReason::RegistryRejected {
                kind: krabka_verified::SchemaFailureKind::Malformed,
                detail: format!("registry returned no schema text for id {id}"),
            });
        };

        // Protobuf carries a message-index between the id and the body, so the
        // body offset depends on the format the id resolved to.
        let (message_index, body) = if schema_body.kind == SchemaKind::Protobuf {
            let (decoded_id, index, body) =
                wire::decode_protobuf(field).map_err(|e| RejectReason::Unframed(e.to_string()))?;
            if decoded_id != id {
                return Err(RejectReason::Unframed(
                    "Protobuf decoder selected a different schema id".to_owned(),
                ));
            }
            (index, body)
        } else {
            (Vec::new(), after_prefix)
        };

        schema_body
            .validate(&message_index, body)
            .map_err(|e| RejectReason::BodyMismatch {
                id,
                detail: e.to_string(),
            })
    }

    /// What an unreachable registry means, per `fail_open`.
    ///
    /// Fail-open covers only the case where the broker could not get an
    /// answer. "Not registered" is an answer, and it rejects under either
    /// setting.
    fn on_unavailable(&self, reason: RejectReason) -> Result<(), RejectReason> {
        let failure = match &reason {
            RejectReason::UnknownId(_) => krabka_verified::SchemaFailureKind::Unknown,
            RejectReason::RegistryUnavailable(_) => krabka_verified::SchemaFailureKind::Transient,
            RejectReason::RegistryRejected { kind, .. } => *kind,
            _ => return Err(reason),
        };
        match krabka_verified::schema_failure_decision(self.fail_open, failure) {
            krabka_verified::SchemaFailureDecision::AllowUnvalidated => Ok(()),
            krabka_verified::SchemaFailureDecision::Reject => Err(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::schema_validation::validator::test_support::{
            ClockedValidator, KNOWN_ID, clocked_validator, configured_validator, framed,
            no_metrics, registry, response_endpoint, status_registry, validator, value_check,
        },
        assert2::{assert, check},
        krabka_units::millis,
        std::{sync::Arc, time::Duration},
        wiremock::{Mock, MockServer, ResponseTemplate, matchers::method},
    };

    /// A registry, a validator against it and a frame it has already
    /// accepted.
    struct BoundSubjectValidator {
        _server: MockServer,
        validator: SchemaValidator,
        field: Vec<u8>,
    }

    async fn bound_subject_validator() -> BoundSubjectValidator {
        let server = registry(1).await;
        let validator = validator(server.uri());
        let field = framed(KNOWN_ID, b"anything");
        check!(
            value_check(&validator, ValidationMode::Id, &field)
                .await
                .is_ok()
        );
        BoundSubjectValidator {
            _server: server,
            validator,
            field,
        }
    }

    async fn json_id_endpoint(server: &MockServer, schema: serde_json::Value) {
        super::super::test_support::schema_versions(server, 1).await;
        response_endpoint(
            server,
            &format!("/schemas/ids/{KNOWN_ID}"),
            ResponseTemplate::new(200).set_body_json(schema),
            Some(1),
        )
        .await;
    }

    async fn fail_open_rejection(uri: String, case: &str) -> RejectReason {
        let validator = configured_validator(uri, true).unwrap();
        let result = value_check(&validator, ValidationMode::Id, &framed(KNOWN_ID, b"x")).await;
        assert!(let Err(reason) = result, "{case}");
        reason
    }

    #[tokio::test]
    async fn a_field_that_carries_no_frame_is_rejected() {
        /// A case name and a field that does not carry a frame.
        struct UnframedField {
            name: &'static str,
            field: Vec<u8>,
        }

        let server = registry(0).await;
        let v = validator(server.uri());
        let cases: Vec<UnframedField> = vec![
            UnframedField {
                name: "plain text",
                field: b"plain text, no serializer".to_vec(),
            },
            UnframedField {
                name: "bad magic",
                field: vec![0x01, 0, 0, 0, 42, b'x'],
            },
            UnframedField {
                name: "truncated id",
                field: vec![0x00, 0, 0],
            },
        ];
        for UnframedField { name, field } in cases {
            let got = value_check(&v, ValidationMode::Id, &field).await;
            assert!(let Err(reason) = got, "case {name}");
            check!(reason.label() == "unframed", "case {name}: {reason}");
        }
    }

    #[tokio::test]
    async fn the_frame_selects_all_four_big_endian_id_bytes() {
        let server = MockServer::start().await;
        response_endpoint(
            &server,
            "/schemas/ids/16909060/versions",
            ResponseTemplate::new(404),
            Some(1),
        )
        .await;
        let v = validator(server.uri());
        let got = value_check(&v, ValidationMode::Id, &[0, 1, 2, 3, 4, b'x']).await;

        assert!(let Err(RejectReason::UnknownId(id)) = got);
        check!(id == 0x0102_0304);
    }

    #[tokio::test]
    async fn an_empty_field_is_accepted() {
        let server = registry(0).await;
        let v = validator(server.uri());
        // Distinct from null on the wire, and some clients write it for an
        // absent value. It cannot carry a frame, so rejecting it would reject
        // those clients for a reason unrelated to schemas.
        check!(value_check(&v, ValidationMode::Id, &[]).await.is_ok());
    }

    #[tokio::test]
    async fn a_bound_id_is_accepted_and_an_unbound_one_is_not() {
        // `expect(1)`: the cache is keyed by schema id, so the second check
        // decides from the cached subject set without a second registry call.
        let BoundSubjectValidator {
            validator: v,
            field,
            ..
        } = bound_subject_validator().await;

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
        let BoundSubjectValidator {
            validator: v,
            field,
            ..
        } = bound_subject_validator().await;
        let err = v
            .check(
                "orders",
                Role::Key,
                ValidationMode::Id,
                &field,
                &no_metrics(),
            )
            .await
            .unwrap_err();
        check!(err.label() == "wrong_subject", "{err}");
    }

    #[tokio::test]
    async fn wrong_subject_is_rejected_before_the_schema_closure_is_fetched() {
        let server = MockServer::start().await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}/versions"),
            ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "other-value", "version": 1}
            ])),
            Some(1),
        )
        .await;
        response_endpoint(
            &server,
            &format!("/schemas/ids/{KNOWN_ID}"),
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": r#"{"type":"record","name":"Envelope","fields":[]}"#,
                "references": [{"name":"Base","subject":"base","version":1}]
            })),
            Some(0),
        )
        .await;

        let result = validator(server.uri())
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &framed(KNOWN_ID, &[]),
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = result);
        check!(reason.label() == "wrong_subject", "{reason}");
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
            value_check(&v, ValidationMode::Id, &field).await.is_ok(),
            "id mode decides from the header alone"
        );
        let got = value_check(&v, ValidationMode::Full, &field).await;
        assert!(let Err(reason) = got);
        check!(reason.label() == "body_mismatch", "{reason}");
    }

    async fn check_full_avro(uri: String, datum: &[u8]) {
        let v = validator(uri);
        let field = framed(KNOWN_ID, datum);
        let result = value_check(&v, ValidationMode::Full, &field).await;
        check!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn full_mode_accepts_a_body_that_matches_its_schema() {
        let server = registry(1).await;
        // Avro string "a": zig-zag length 1 followed by its byte.
        check_full_avro(server.uri(), &[0x02, b'a']).await;
    }

    #[tokio::test]
    async fn full_mode_resolves_avro_references_before_validating() {
        let server = super::super::test_support::referenced_avro(
            r#"{"type":"record","name":"Envelope","fields":[{"name":"base","type":"Base"}]}"#,
        )
        .await;

        check_full_avro(server.uri(), &[0x02, b'a']).await;
    }

    #[tokio::test]
    async fn full_mode_accepts_an_unnamed_avro_root_with_a_reference() {
        let server =
            super::super::test_support::referenced_avro(r#"{"type":"array","items":"Base"}"#).await;

        let v = validator(server.uri());
        // One-element array, one record containing "a", then the array terminator.
        let field = framed(KNOWN_ID, &[0x02, 0x02, b'a', 0x00]);
        let result = value_check(&v, ValidationMode::Full, &field).await;
        check!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn full_mode_resolves_json_schema_references_before_validating() {
        let server = MockServer::start().await;
        json_id_endpoint(&server, serde_json::json!({
                "schemaType": "JSON",
                "schema": r#"{"$id":"https://schemas.example/root.json","$ref":"base.json"}"#,
                "references": [{"name":"base.json","subject":"order-base","version":1}]
})).await;
        response_endpoint(&server, "/subjects/order-base/versions/1", ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "order-base",
                "version": 1,
                "id": 7,
                "schemaType": "JSON",
                "schema": r#"{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"#
            })), Some(1)).await;

        let v = validator(server.uri());
        let valid = framed(KNOWN_ID, br#"{"id":"a"}"#);
        let valid_result = value_check(&v, ValidationMode::Full, &valid).await;
        check!(valid_result.is_ok(), "{valid_result:?}");

        let invalid = framed(KNOWN_ID, br#"{"id":1}"#);
        let result = value_check(&v, ValidationMode::Full, &invalid).await;
        assert!(let Err(reason) = result);
        check!(reason.label() == "body_mismatch", "{reason}");

        // The second rejection comes from the compiled validator the first
        // one built, and reads the same.
        let again = value_check(&v, ValidationMode::Full, &invalid).await;
        check!(again == Err(reason));
        check!(compilations(&v).await == 1);
    }

    /// How many times the cached schema closure for `KNOWN_ID` was compiled.
    async fn compilations(v: &SchemaValidator) -> usize {
        v.entry(
            KNOWN_ID,
            "orders-value",
            ValidationMode::Full,
            &no_metrics(),
        )
        .await
        .expect("cached lookup")
        .body
        .as_ref()
        .expect("full lookup has a body")
        .compilations()
    }

    /// Check a matching and a mismatched record three times each: every
    /// answer must repeat the first, and the schema must compile only once.
    async fn checks_compile_once(v: &SchemaValidator, matching: &[u8], mismatched: &[u8]) {
        let mut rejections = Vec::new();
        for _ in 0..3 {
            let accepted = value_check(v, ValidationMode::Full, matching).await;
            check!(accepted.is_ok(), "{accepted:?}");
            assert!(let Err(reason) = value_check(v, ValidationMode::Full, mismatched).await);
            check!(reason.label() == "body_mismatch", "{reason}");
            rejections.push(reason);
        }

        check!(rejections.windows(2).all(|pair| pair[0] == pair[1]));
        check!(compilations(v).await == 1);
    }

    #[tokio::test]
    async fn a_schema_is_compiled_once_for_every_record_that_names_it() {
        let server = registry(1).await;
        let v = validator(server.uri());
        // Avro string "a", and bytes that are no datum of the schema.
        let matching = framed(KNOWN_ID, &[0x02, b'a']);
        let mismatched = framed(KNOWN_ID, &[0xFF; 6]);

        checks_compile_once(&v, &matching, &mismatched).await;
    }

    #[tokio::test]
    async fn a_protobuf_schema_with_references_is_compiled_once() {
        let server = MockServer::start().await;
        json_id_endpoint(
            &server,
            serde_json::json!({
                "schemaType": "PROTOBUF",
                "schema": "syntax = \"proto3\"; import \"money.proto\"; message Order { money.Money total = 1; }",
                "references": [{"name":"money.proto","subject":"money","version":1}]
            }),
        )
        .await;
        response_endpoint(
            &server,
            "/subjects/money/versions/1",
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "money",
                "version": 1,
                "id": 7,
                "schemaType": "PROTOBUF",
                "schema": "syntax = \"proto3\"; package money; message Money { int64 cents = 1; }"
            })),
            Some(1),
        )
        .await;
        let v = validator(server.uri());
        // Message-index `[0]`, written as Confluent's single zero byte, then
        // `Order { total: Money { cents: 19 } }`.
        let matching = framed(KNOWN_ID, &[0x00, 0x0A, 0x02, 0x08, 0x13]);
        // Field number 0 is illegal in every protobuf message.
        let mismatched = framed(KNOWN_ID, &[0x00, 0x00, 0x01]);

        checks_compile_once(&v, &matching, &mismatched).await;
    }

    #[tokio::test]
    async fn an_expired_entry_compiles_its_schema_afresh() {
        // Two registry answers, one per TTL, and one compile for each: the
        // compiled form lives and dies with the cache entry that holds it.
        let server = registry(2).await;
        let ClockedValidator {
            timeline,
            validator: v,
        } = clocked_validator(server.uri(), millis(10));
        let field = framed(KNOWN_ID, &[0x02, b'a']);

        check!(value_check(&v, ValidationMode::Full, &field).await.is_ok());
        let before = v
            .entry(
                KNOWN_ID,
                "orders-value",
                ValidationMode::Full,
                &no_metrics(),
            )
            .await
            .expect("cached lookup");
        timeline
            .advance(Duration::from_millis(50))
            .expect("manual time moves forward");
        check!(value_check(&v, ValidationMode::Full, &field).await.is_ok());
        let after = v
            .entry(
                KNOWN_ID,
                "orders-value",
                ValidationMode::Full,
                &no_metrics(),
            )
            .await
            .expect("refreshed lookup");

        assert!(let (Some(before), Some(after)) = (&before.body, &after.body));
        check!(!Arc::ptr_eq(before, after));
        check!(before.compilations() == 1);
        check!(after.compilations() == 1);
    }

    #[tokio::test]
    async fn full_mode_never_fetches_json_references_outside_the_registry_cache() {
        let server = MockServer::start().await;
        json_id_endpoint(
            &server,
            serde_json::json!({
                            "schemaType": "JSON",
                            "schema": format!(r#"{{"$ref":"{}/outside.json"}}"#, server.uri())
            }),
        )
        .await;
        response_endpoint(
            &server,
            "/outside.json",
            ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
            Some(0),
        )
        .await;

        let result = validator(server.uri())
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &framed(KNOWN_ID, b"{}"),
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = result);
        check!(reason.label() == "body_mismatch", "{reason}");
    }

    #[tokio::test]
    async fn an_unreachable_registry_fails_closed_or_open_by_the_knob() {
        let server = status_registry(500).await;
        let field = framed(KNOWN_ID, b"anything");

        let closed = configured_validator(server.uri(), false).unwrap();
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

        let open = configured_validator(server.uri(), true).unwrap();
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
        let server = status_registry(404).await;

        // 404 is the registry answering, not failing to answer. `fail_open`
        // governs only the second case.
        let reason = fail_open_rejection(server.uri(), "an unregistered id").await;
        check!(reason.label() == "unknown_id", "{reason}");
    }

    #[yare::parameterized(
        bad_request = { 400 },
        unauthorized = { 401 },
        forbidden = { 403 },
        invalid_status = { 600 },
    )]
    #[test_macro(tokio::test)]
    async fn fail_open_rejects_permanent_registry_errors(status: u16) {
        let server = status_registry(status).await;

        let reason = fail_open_rejection(server.uri(), &format!("status {status}")).await;
        check!(
            reason.label() == "registry_unavailable",
            "status {status}: {reason}"
        );
    }

    #[tokio::test]
    async fn fail_open_rejects_a_malformed_success_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let reason = fail_open_rejection(server.uri(), "a malformed success").await;
        check!(reason.label() == "registry_unavailable", "{reason}");
    }

    #[yare::parameterized(
        request_timeout = { 408 },
        too_many_requests = { 429 },
    )]
    #[test_macro(tokio::test)]
    async fn fail_open_admits_retryable_registry_statuses(status: u16) {
        let server = status_registry(status).await;

        let v = configured_validator(server.uri(), true).unwrap();
        check!(
            value_check(&v, ValidationMode::Id, &framed(KNOWN_ID, b"x"))
                .await
                .is_ok(),
            "status {status}"
        );
    }
}
