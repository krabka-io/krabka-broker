//! The per-record check: the Confluent frame, the subject binding, and — in
//! full mode — the body decode against the schema the frame names.
//!
//! The body step is where the format matters, because Protobuf puts a
//! message-index between the id and the payload and Avro and JSON Schema do
//! not. That per-format detail, and the `fail_open` decision that a registry
//! failure runs into, are what this module holds; the lookup behind it is in
//! `cache`.

use krabka_schema_serde::{
    error::SchemaSerdeError,
    format::validate::{validate_body, validate_protobuf_with_references},
    subject::{Role, SchemaKind, SubjectStrategy as _, TopicNameStrategy},
    wire,
};
use std::collections::HashMap;

use super::{SchemaValidator, reject::RejectReason};
use crate::{metrics::BrokerMetrics, schema_validation::ValidationMode};

impl SchemaValidator {
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

        let [magic, id_0, id_1, id_2, id_3, ..] = field else {
            return Err(RejectReason::Unframed(
                "Confluent frame is shorter than its five-byte prefix".to_owned(),
            ));
        };
        let Some(id) = krabka_verified::schema_frame_id(*magic, *id_0, *id_1, *id_2, *id_3) else {
            return Err(RejectReason::Unframed(format!(
                "unsupported Confluent magic byte {magic}"
            )));
        };
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
            (Vec::new(), &field[5..])
        };

        validate_body_with_references(
            schema_body.kind,
            &schema_body.schema,
            &schema_body.references,
            &schema_body.reference_order,
            &message_index,
            body,
        )
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

fn validate_body_with_references(
    kind: SchemaKind,
    schema: &str,
    references: &HashMap<String, String>,
    reference_order: &[String],
    message_index: &[i32],
    body: &[u8],
) -> Result<(), SchemaSerdeError> {
    match kind {
        SchemaKind::Avro if !references.is_empty() => {
            let sources: Vec<_> = reference_order
                .iter()
                .filter_map(|name| references.get(name).map(String::as_str))
                .collect();
            let (writer, schemas) = apache_avro::Schema::parse_str_with_list(schema, sources)
                .map_err(|error| SchemaSerdeError::Schema(error.to_string()))?;
            let mut cursor = body;
            apache_avro::from_avro_datum_schemata(
                &writer,
                schemas.iter().collect(),
                &mut cursor,
                None,
            )
            .map_err(|error| SchemaSerdeError::Deserialize(format!("avro body: {error}")))?;
            if cursor.is_empty() {
                Ok(())
            } else {
                Err(SchemaSerdeError::Deserialize(format!(
                    "avro body: {} trailing byte(s) after the datum",
                    cursor.len()
                )))
            }
        }
        SchemaKind::Protobuf if !references.is_empty() => {
            validate_protobuf_with_references(schema, references, message_index, body)
        }
        SchemaKind::Json if !references.is_empty() => {
            let writer: serde_json::Value = serde_json::from_str(schema)
                .map_err(|error| SchemaSerdeError::Schema(error.to_string()))?;
            let instance: serde_json::Value = serde_json::from_slice(body)
                .map_err(|error| SchemaSerdeError::Deserialize(error.to_string()))?;
            let references = references
                .iter()
                .map(|(name, source)| {
                    serde_json::from_str(source)
                        .map(|schema| (name.clone(), schema))
                        .map_err(|error| SchemaSerdeError::Schema(error.to_string()))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            let validator = jsonschema::options()
                .with_retriever(CachedJsonSchemas(references))
                .build(&writer)
                .map_err(|error| SchemaSerdeError::Schema(error.to_string()))?;
            validator.validate(&instance).map_err(|error| {
                SchemaSerdeError::Deserialize(format!("json schema validation: {error}"))
            })
        }
        _ => validate_body(kind, schema, message_index, body),
    }
}

#[derive(Debug)]
struct CachedJsonSchemas(HashMap<String, serde_json::Value>);

impl jsonschema::Retrieve for CachedJsonSchemas {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.get(uri.as_str()).cloned().ok_or_else(|| {
            format!("JSON Schema reference {uri} is not present in the registry cache").into()
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{minutes, secs};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::schema_validation::validator::test_support::{
        KNOWN_ID, framed, no_metrics, registry, validator,
    };

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
    async fn the_frame_selects_all_four_big_endian_id_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/16909060/versions"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let v = validator(server.uri());
        let got = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Id,
                &[0, 1, 2, 3, 4, b'x'],
                &no_metrics(),
            )
            .await;

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
        let result = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &field,
                &no_metrics(),
            )
            .await;
        check!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn full_mode_resolves_avro_references_before_validating() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": r#"{"type":"record","name":"Envelope","fields":[{"name":"base","type":"Base"}]}"#,
                "references": [{"name":"Base","subject":"order-base","version":1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/order-base/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "order-base",
                "version": 1,
                "id": 7,
                "schema": r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#
            })))
            .expect(1)
            .mount(&server)
            .await;

        let v = validator(server.uri());
        let field = framed(KNOWN_ID, &[0x02, b'a']);
        let result = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &field,
                &no_metrics(),
            )
            .await;
        check!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn full_mode_accepts_an_unnamed_avro_root_with_a_reference() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": r#"{"type":"array","items":"Base"}"#,
                "references": [{"name":"Base","subject":"order-base","version":1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/order-base/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "order-base",
                "version": 1,
                "id": 7,
                "schema": r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#
            })))
            .expect(1)
            .mount(&server)
            .await;

        let v = validator(server.uri());
        // One-element array, one record containing "a", then the array terminator.
        let field = framed(KNOWN_ID, &[0x02, 0x02, b'a', 0x00]);
        let result = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &field,
                &no_metrics(),
            )
            .await;
        check!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn full_mode_resolves_json_schema_references_before_validating() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 1}
            ])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{KNOWN_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": "JSON",
                "schema": r#"{"$ref":"https://schemas.example/base.json"}"#,
                "references": [{"name":"https://schemas.example/base.json","subject":"order-base","version":1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/order-base/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "order-base",
                "version": 1,
                "id": 7,
                "schemaType": "JSON",
                "schema": r#"{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"#
            })))
            .expect(1)
            .mount(&server)
            .await;

        let v = validator(server.uri());
        let valid = framed(KNOWN_ID, br#"{"id":"a"}"#);
        let valid_result = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &valid,
                &no_metrics(),
            )
            .await;
        check!(valid_result.is_ok(), "{valid_result:?}");

        let invalid = framed(KNOWN_ID, br#"{"id":1}"#);
        let result = v
            .check(
                "orders",
                Role::Value,
                ValidationMode::Full,
                &invalid,
                &no_metrics(),
            )
            .await;
        assert!(let Err(reason) = result);
        check!(reason.label() == "body_mismatch", "{reason}");
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

    #[tokio::test]
    async fn fail_open_rejects_permanent_registry_errors() {
        for status in [400, 401, 403, 600] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;

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
            assert!(let Err(reason) = got, "status {status}");
            check!(
                reason.label() == "registry_unavailable",
                "status {status}: {reason}"
            );
        }
    }

    #[tokio::test]
    async fn fail_open_rejects_a_malformed_success_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

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
        check!(reason.label() == "registry_unavailable", "{reason}");
    }

    #[tokio::test]
    async fn fail_open_admits_retryable_registry_statuses() {
        for status in [408, 429] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;

            let v = SchemaValidator::new(server.uri(), true, 100, minutes(1), secs(5)).unwrap();
            check!(
                v.check(
                    "orders",
                    Role::Value,
                    ValidationMode::Id,
                    &framed(KNOWN_ID, b"x"),
                    &no_metrics(),
                )
                .await
                .is_ok(),
                "status {status}"
            );
        }
    }
}
