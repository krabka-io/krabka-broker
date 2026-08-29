//! The audit trail for `DeleteTopics`.
//!
//! Only topics that actually went away are auditable, so the response rows are
//! filtered down to the successful, named ones before a single
//! `AdminOperation` record is emitted for the request.

use krabka_protocol::owned::delete_topics_response::DeletableTopicResult;

use crate::codes;

/// Picks the audit resources for the topics this request actually deleted.
///
/// A row with a non-zero error code did not delete anything, and a row without
/// a name was requested by an id that resolved to nothing.
pub(super) fn deleted_topic_resources(
    results: &[DeletableTopicResult],
) -> Vec<krabka_audit::AuditResource> {
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .filter_map(|t| {
            t.name.as_deref().map(|n| krabka_audit::AuditResource {
                resource_type: "Topic".to_string(),
                name: n.to_string(),
            })
        })
        .collect()
}

/// Emits one `AdminOperation` audit record for the deleted topics.
///
/// A request that deleted nothing emits nothing.
pub(super) fn audit_deleted_topics(
    audit_log: &krabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    deleted: Vec<krabka_audit::AuditResource>,
) {
    if !deleted.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "DeleteTopics",
            krabka_audit::AuditOutcome::Success,
            deleted,
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;

    use super::*;
    use crate::{
        handlers::delete_topics::wire::delete_topic_result,
        test_support::{peer, principal},
    };

    /// The `RequestContext` the `DeleteTopics` tests share, over the same
    /// `admin-client` client id the handler tests drive the wire path with.
    fn test_context<'a>(
        principal: &'a krabka_security::Principal,
        peer: &'a std::net::SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    #[test]
    fn deleted_topic_resources_include_only_successful_named_topics() {
        let results = vec![
            delete_topic_result(Some("ok".into()), WireUuid::ZERO, codes::NONE),
            delete_topic_result(
                Some("denied".into()),
                WireUuid::ZERO,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ),
            delete_topic_result(None, WireUuid([1; 16]), codes::NONE),
        ];

        let resources = deleted_topic_resources(&results);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_deleted_topics_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_deleted_topics(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_deleted_topics(
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
        let expected_resources = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "orders".into(),
        }];
        check!(outcome == krabka_audit::AuditOutcome::Success);
        check!(principal.name.as_str() == "admin");
        check!(operation.as_str() == "DeleteTopics");
        check!(resources == expected_resources);
    }
}
