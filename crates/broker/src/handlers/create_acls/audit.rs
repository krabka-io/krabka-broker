//! The audit trail for `CreateAcls`: which creations actually reached the
//! metadata log, and the single `AdminOperation` event they produce.
//!
//! A creation counts as audited only when it passed validation and its result
//! row still holds `NONE` after the controller submit, so the selection is a
//! join over the request, the results, and the submitted records. That join is
//! worth its own file.

use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    create_acls_request::CreateAclsRequest, create_acls_response::AclCreationResult,
};

use crate::codes;

pub(super) fn created_acl_resources(
    req: &CreateAclsRequest,
    results: &[AclCreationResult],
    to_submit: &[(usize, MetadataRecord)],
) -> Vec<krabka_audit::AuditResource> {
    to_submit
        .iter()
        .filter(|(idx, _)| results[*idx].error_code == codes::NONE)
        .map(|(idx, _)| krabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: req.creations[*idx].resource_name.clone(),
        })
        .collect()
}

pub(super) fn audit_created_acls(
    audit_log: &krabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    created_acls: Vec<krabka_audit::AuditResource>,
) {
    if !created_acls.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "CreateAcls",
            krabka_audit::AuditOutcome::Success,
            created_acls,
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        handlers::create_acls::{
            response::acl_error_result,
            test_support::{
                OPERATION_READ, OPERATION_WRITE, creation, request, test_context, validate,
            },
        },
        test_support::{peer, principal},
    };

    #[test]
    fn created_acl_resources_include_only_successful_submitted_creations() {
        let req = request(vec![
            creation("topic-ok", "User:alice", OPERATION_READ),
            creation("topic-bad", "User:bob", OPERATION_WRITE),
        ]);
        let submitted = vec![
            (
                0usize,
                MetadataRecord::V1AccessControlEntry(validate(&req.creations[0]).unwrap()),
            ),
            (
                1usize,
                MetadataRecord::V1AccessControlEntry(validate(&req.creations[1]).unwrap()),
            ),
        ];
        let results = vec![
            AclCreationResult::default(),
            acl_error_result(codes::COORDINATOR_NOT_AVAILABLE, "submit failed"),
        ];

        let resources = created_acl_resources(&req, &results, &submitted);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: "topic-ok".to_string(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_created_acls_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_created_acls(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_created_acls(
            log.as_ref(),
            &ctx,
            vec![krabka_audit::AuditResource {
                resource_type: "Acl".into(),
                name: "topic-ok".into(),
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
                resources,
            ) == (
                krabka_audit::AuditOutcome::Success,
                "admin",
                "CreateAcls",
                vec![krabka_audit::AuditResource {
                    resource_type: "Acl".to_string(),
                    name: "topic-ok".to_string(),
                }],
            )
        );
    }
}
