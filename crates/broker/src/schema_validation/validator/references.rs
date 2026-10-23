//! Bounded registry reference traversal in dependency order.
//!
//! The caller owns the operation deadline, including all nested requests.

use {
    krabka_schema_serde::{
        error::SchemaSerdeError,
        registry::{RegistryClient, model::SchemaReference},
    },
    std::collections::{HashMap, HashSet},
};

const MAX_REFERENCE_SCHEMAS: usize = 32;
pub(super) const MAX_REFERENCE_BYTES: usize = 1024 * 1024;

/// A subject at one version, as a schema reference names it.
#[derive(Clone, PartialEq, Eq, Hash)]
struct SubjectVersion {
    subject: String,
    version: i32,
}

/// The schema sources a root schema's references resolve to, and the order
/// they must be loaded in.
pub(super) struct ReferenceSources {
    pub(super) sources: HashMap<String, String>,
    pub(super) order: Vec<String>,
}

struct ReferenceClosure {
    sources: HashMap<String, String>,
    order: Vec<String>,
    resolving: HashSet<SubjectVersion>,
    fetched_count: usize,
    fetched_bytes: usize,
    remaining_bytes: usize,
}

pub(super) async fn reference_sources_ordered(
    client: &RegistryClient,
    references: &[SchemaReference],
    remaining_bytes: usize,
) -> Result<ReferenceSources, SchemaSerdeError> {
    let mut closure = ReferenceClosure {
        sources: HashMap::new(),
        order: Vec::new(),
        resolving: HashSet::new(),
        fetched_count: 0,
        fetched_bytes: 0,
        remaining_bytes,
    };
    resolve_reference_sources(client, references, &mut closure, 0).await?;
    Ok(ReferenceSources {
        sources: closure.sources,
        order: closure.order,
    })
}

async fn resolve_reference_sources(
    client: &RegistryClient,
    references: &[SchemaReference],
    closure: &mut ReferenceClosure,
    depth: usize,
) -> Result<(), SchemaSerdeError> {
    const MAX_REFERENCE_DEPTH: usize = 32;
    if !references.is_empty() && depth >= MAX_REFERENCE_DEPTH {
        return Err(SchemaSerdeError::Schema(format!(
            "schema reference depth exceeds {MAX_REFERENCE_DEPTH}"
        )));
    }
    for reference in references {
        if closure.sources.contains_key(&reference.name) {
            continue;
        }
        let key = SubjectVersion {
            subject: reference.subject.clone(),
            version: reference.version,
        };
        if !closure.resolving.insert(key.clone()) {
            return Err(SchemaSerdeError::Schema(format!(
                "cyclic schema reference at {} version {}",
                reference.subject, reference.version
            )));
        }
        if closure.fetched_count >= MAX_REFERENCE_SCHEMAS {
            return Err(SchemaSerdeError::Schema(format!(
                "schema closure exceeds {MAX_REFERENCE_SCHEMAS} references"
            )));
        }
        closure.fetched_count += 1;
        let fetched = client
            .schema_by_subject_version(&reference.subject, reference.version)
            .await?;
        closure.fetched_bytes = closure
            .fetched_bytes
            .checked_add(fetched.schema.len())
            .filter(|bytes| *bytes <= closure.remaining_bytes)
            .ok_or_else(|| {
                SchemaSerdeError::Schema(format!(
                    "schema closure exceeds {MAX_REFERENCE_BYTES} bytes"
                ))
            })?;
        Box::pin(resolve_reference_sources(
            client,
            &fetched.references,
            closure,
            depth + 1,
        ))
        .await?;
        closure.resolving.remove(&key);
        closure
            .sources
            .insert(reference.name.clone(), fetched.schema);
        closure.order.push(reference.name.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::schema_validation::validator::test_support::response_endpoint,
        assert2::{assert, check},
        wiremock::{MockServer, ResponseTemplate},
    };

    fn reference(index: usize) -> SchemaReference {
        SchemaReference {
            name: format!("Ref{index}"),
            subject: format!("subject-{index}"),
            version: 1,
        }
    }

    async fn reference_endpoint(
        server: &MockServer,
        index: usize,
        references: Option<serde_json::Value>,
        expected_calls: u64,
    ) {
        let mut body = serde_json::json!({
            "subject": format!("subject-{index}"),
            "version": 1,
            "id": index,
            "schema": r#"{"type":"record","name":"Leaf","fields":[]}"#,
        });
        if let Some(references) = references {
            body["references"] = references;
        }
        response_endpoint(
            server,
            &format!("/subjects/subject-{index}/versions/1"),
            ResponseTemplate::new(200).set_body_json(body),
            Some(expected_calls),
        )
        .await;
    }

    #[tokio::test]
    async fn a_reference_chain_may_end_at_the_depth_limit() {
        const DEPTH: usize = 32;
        let server = MockServer::start().await;
        for index in 0..DEPTH {
            let references = if index + 1 == DEPTH {
                serde_json::json!([])
            } else {
                serde_json::json!([{
                    "name": format!("Ref{}", index + 1),
                    "subject": format!("subject-{}", index + 1),
                    "version": 1
                }])
            };
            reference_endpoint(&server, index, Some(references), 1).await;
        }

        let client = RegistryClient::with_http_client(server.uri(), reqwest::Client::new());
        let root = [reference(0)];
        let result = reference_sources_ordered(&client, &root, MAX_REFERENCE_BYTES).await;
        assert!(let Ok(ReferenceSources { sources, order }) = result);
        check!(sources.len() == DEPTH);
        check!(
            order
                == (0..DEPTH)
                    .rev()
                    .map(|index| format!("Ref{index}"))
                    .collect::<Vec<_>>()
        );
    }

    #[yare::parameterized(
        cycle = { true, "cyclic schema reference at subject-0 version 1" },
        depth = { false, "schema reference depth exceeds 32" },
    )]
    #[test_macro(tokio::test)]
    async fn a_cycle_or_excess_depth_is_rejected(cycle: bool, expected: &str) {
        use assert2::assert;

        let server = MockServer::start().await;
        let count = if cycle { 1 } else { 32 };
        for index in 0..count {
            let next = if cycle { 0 } else { index + 1 };
            reference_endpoint(
                &server,
                index,
                Some(serde_json::json!([{
                    "name": format!("Ref{next}"),
                    "subject": format!("subject-{next}"),
                    "version": 1
                }])),
                1,
            )
            .await;
        }
        let client = RegistryClient::with_http_client(server.uri(), reqwest::Client::new());
        let root = [reference(0)];
        let error = reference_sources_ordered(&client, &root, MAX_REFERENCE_BYTES)
            .await
            .err()
            .expect("invalid graph");
        assert!(let SchemaSerdeError::Schema(detail) = error);
        check!(detail == expected);
    }

    #[derive(Clone, Copy)]
    enum ClosureLimit {
        SchemaCount,
        TotalBytes,
    }

    #[yare::parameterized(
        schema_count = { ClosureLimit::SchemaCount },
        total_bytes = { ClosureLimit::TotalBytes },
    )]
    #[test_macro(tokio::test)]
    async fn a_reference_closure_is_bounded(limit: ClosureLimit) {
        let server = MockServer::start().await;
        let (root, expected_error) = match limit {
            ClosureLimit::SchemaCount => {
                let mut root = Vec::new();
                for index in 0..=MAX_REFERENCE_SCHEMAS {
                    root.push(reference(index));
                    reference_endpoint(
                        &server,
                        index,
                        None,
                        u64::from(index < MAX_REFERENCE_SCHEMAS),
                    )
                    .await;
                }
                (root, "schema closure exceeds 32 references")
            }
            ClosureLimit::TotalBytes => {
                response_endpoint(
                    &server,
                    "/subjects/large/versions/1",
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "subject": "large",
                        "version": 1,
                        "id": 7,
                        "schema": "x".repeat(MAX_REFERENCE_BYTES + 1)
                    })),
                    Some(1),
                )
                .await;
                (
                    vec![SchemaReference {
                        name: "large".to_owned(),
                        subject: "large".to_owned(),
                        version: 1,
                    }],
                    "schema closure exceeds 1048576 bytes",
                )
            }
        };

        let client = RegistryClient::with_http_client(server.uri(), reqwest::Client::new());
        let result = reference_sources_ordered(&client, &root, MAX_REFERENCE_BYTES).await;
        let Err(error) = result else {
            panic!("expected the oversized reference closure to be rejected");
        };
        check!(error.to_string().contains(expected_error), "{error}");
    }
}
