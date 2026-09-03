//! Response assembly for `CreateTopics`: the per-topic result rows, the
//! response envelope around them, the admin audit event for the topics that
//! were created, and the KIP-219 throttle window recorded on the request
//! context for the connection loop to enforce after the reply is written.

use bytes::Bytes;
use krabka_protocol::{
    Encode,
    api_key::ApiKey,
    owned::create_topics_response::{
        CreatableTopicConfigs, CreatableTopicResult, CreateTopicsResponse,
    },
};
use krabka_units::Time;

use crate::{broker::Broker, codes, error::BrokerError};

pub(super) fn topic_error_result(
    name: String,
    error_code: i16,
    error_message: Option<String>,
) -> CreatableTopicResult {
    CreatableTopicResult {
        name,
        error_code,
        error_message,
        ..Default::default()
    }
}

/// KIP-525: the topic's effective configuration, in the shape a v5+
/// `CreatableTopicResult` carries it.
///
/// It is the very list `DescribeConfigs` answers a `TOPIC` resource with --
/// Kafka reaches `ConfigurationControlManager.computeEffectiveTopicConfigs`
/// from both paths, and a client that reads `createTopics(...).config(topic)`
/// instead of issuing a follow-up `DescribeConfigs` must see the same values.
/// `CreatableTopicConfigs` has no `config_type`, `documentation` or synonym
/// field, so those parts of the entry are dropped and the rest travels
/// unchanged, sensitive values withheld included.
///
/// `read_only` is the one place krabka reports more than Kafka does. Kafka
/// hardcodes `false` here (`KafkaConfigSchema.toConfigEntry`, "readonly is
/// always false, for now") because no topic key in its schema is read-only.
/// Two of krabka's are -- [`crate::config_keys::DISKLESS`] and
/// [`crate::config_keys::WRITE_FREEZE`] -- and reporting them as writable
/// would contradict this broker's own `DescribeConfigs` and its two alter
/// paths.
pub(super) fn effective_topic_configs(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Vec<CreatableTopicConfigs> {
    crate::handlers::describe_configs::effective_topic_configs(image, topic)
        .into_iter()
        .map(|entry| CreatableTopicConfigs {
            name: entry.name,
            value: entry.value,
            read_only: entry.read_only,
            config_source: entry.config_source,
            is_sensitive: entry.is_sensitive,
            ..Default::default()
        })
        .collect()
}

pub(super) fn create_topics_response(
    topics: Vec<CreatableTopicResult>,
    throttle_time_ms: i32,
) -> CreateTopicsResponse {
    CreateTopicsResponse {
        topics,
        throttle_time_ms,
        ..Default::default()
    }
}

pub(super) fn created_topic_resources(
    results: &[CreatableTopicResult],
) -> Vec<krabka_audit::AuditResource> {
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .map(|t| krabka_audit::AuditResource {
            resource_type: "Topic".to_string(),
            name: t.name.clone(),
        })
        .collect()
}

pub(super) fn audit_created_topics(
    audit_log: &krabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    created: Vec<krabka_audit::AuditResource>,
) {
    if !created.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "CreateTopics",
            krabka_audit::AuditOutcome::Success,
            created,
        );
    }
}

pub(super) fn encode_response<R: Encode>(resp: &R, version: i16) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

pub(super) fn finish_response(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    results: Vec<CreatableTopicResult>,
    delay: Time,
    version: i16,
) -> Result<Bytes, BrokerError> {
    audit_created_topics(
        broker.audit_log.as_ref(),
        context,
        created_topic_resources(&results),
    );
    // The KIP-599 delay is the only throttle this api applies — the dispatch
    // loop marks it quota-exempt and never charges it the request quota — so
    // resolving it through the metric records the throttle phase and the quota
    // that caused it exactly once per request.
    let delay = broker.metrics.record_applied_throttle(
        ApiKey::CreateTopics as i16,
        &[(crate::metrics::QuotaType::ControllerMutation, delay)],
    );
    let response = create_topics_response(results, crate::quota::throttle_time_ms(delay));
    // KIP-219: the KIP-599 window is reported here and enforced by the
    // connection loop, which mutes the connection after the response is sent.
    context.record_throttle(delay);
    encode_response(&response, version)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::test_support::{peer, principal};

    // The context that `crate::test_support::wire_helpers!` builds for the
    // handler tests. These two cases need the context but no wire codec.
    fn test_context<'a>(
        principal: &'a krabka_security::Principal,
        peer: &'a std::net::SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    #[test]
    fn created_topic_resources_include_only_successful_topics() {
        let results = vec![
            CreatableTopicResult {
                name: "ok".into(),
                error_code: codes::NONE,
                ..Default::default()
            },
            CreatableTopicResult {
                name: "bad".into(),
                error_code: codes::TOPIC_ALREADY_EXISTS,
                ..Default::default()
            },
        ];

        let resources = created_topic_resources(&results);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_created_topics_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_created_topics(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_created_topics(
            log.as_ref(),
            &ctx,
            vec![krabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        let event = rx.try_recv().expect("admin audit event");
        let krabka_audit::AuditEvent::AdminOperation {
            outcome,
            principal,
            operation,
            resources,
            ..
        } = event
        else {
            panic!("expected AdminOperation");
        };
        check!(outcome == krabka_audit::AuditOutcome::Success);
        check!(principal.name.as_str() == "admin");
        check!(operation.as_str() == "CreateTopics");
        let expected_resources = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "orders".into(),
        }];
        assert!(resources == expected_resources);
    }
}
