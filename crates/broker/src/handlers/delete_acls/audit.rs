//! The audit trail for `DeleteAcls`: which of the matched ACL entries actually
//! left the metadata log, and the single `AdminOperation` event they produce.
//!
//! A deletion counts as audited only when its filter result still holds `NONE`
//! after the controller submit, so the selection is a scan over the response
//! rows rather than over the request. That join is worth its own file.

use krabka_protocol::owned::delete_acls_response::DeleteAclsFilterResult;

use crate::codes;

pub(super) fn deleted_acl_resources(
    filter_results: &[DeleteAclsFilterResult],
) -> Vec<krabka_audit::AuditResource> {
    filter_results
        .iter()
        .filter(|r| r.error_code == codes::NONE)
        .flat_map(|r| r.matching_acls.iter())
        .map(|m| krabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: m.resource_name.clone(),
        })
        .collect()
}

pub(super) fn audit_deleted_acls(
    audit_log: &krabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    deleted_acls: Vec<krabka_audit::AuditResource>,
) {
    if !deleted_acls.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "DeleteAcls",
            krabka_audit::AuditOutcome::Success,
            deleted_acls,
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::AclOperation;

    use super::*;
    use crate::{
        handlers::delete_acls::{
            response::{filter_result, matching_acl_result},
            test_support::{acl, test_context},
        },
        test_support::{peer, principal},
    };

    #[test]
    fn deleted_acl_resources_include_only_successful_matches() {
        let ok = filter_result(
            codes::NONE,
            None,
            vec![matching_acl_result(&acl(
                "orders",
                "User:alice",
                AclOperation::Read,
            ))],
        );
        let failed = filter_result(
            codes::COORDINATOR_NOT_AVAILABLE,
            Some("submit failed".into()),
            vec![matching_acl_result(&acl(
                "payments",
                "User:bob",
                AclOperation::Write,
            ))],
        );

        let resources = deleted_acl_resources(&[ok, failed]);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Acl".into(),
            name: "orders".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_deleted_acls_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_deleted_acls(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_deleted_acls(
            log.as_ref(),
            &ctx,
            vec![krabka_audit::AuditResource {
                resource_type: "Acl".into(),
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
        assert!(
            (
                outcome,
                principal.name.as_str(),
                operation.as_str(),
                resources
            ) == (
                krabka_audit::AuditOutcome::Success,
                "admin",
                "DeleteAcls",
                vec![krabka_audit::AuditResource {
                    resource_type: "Acl".into(),
                    name: "orders".into(),
                }],
            )
        );
    }
}
