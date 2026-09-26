//! Construction of the `AlterClientQuotas` response rows.
//!
//! One place builds every row shape the handler can return: an accepted entry,
//! an entry that carries an error code and a message, the rewrite that turns
//! the rows still marked accepted into a coordinator error when the metadata
//! submit fails, and the whole-request error body an authorization denial
//! produces.

use bytes::Bytes;
use krabka_metadata::EntityKey;
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        alter_client_quotas_request::AlterClientQuotasRequest,
        alter_client_quotas_response::{
            AlterClientQuotasResponse, EntityData as RespEntity, EntryData as RespEntry,
        },
    },
};

use crate::codes::{COORDINATOR_NOT_AVAILABLE, NONE};

fn entity_rows(entity: &[(String, Option<String>)]) -> Vec<RespEntity> {
    entity
        .iter()
        .map(|(entity_type, entity_name)| RespEntity {
            entity_type: entity_type.clone(),
            entity_name: entity_name.clone(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect()
}

pub(super) fn ok_entry(entity: &[(String, Option<String>)]) -> RespEntry {
    RespEntry {
        error_code: NONE,
        error_message: None,
        entity: entity_rows(entity),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn err_entry(entity: &[(String, Option<String>)], code: i16, msg: String) -> RespEntry {
    RespEntry {
        error_code: code,
        error_message: Some(msg),
        entity: entity_rows(entity),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn apply_submit_error(entry_results: &mut [RespEntry], error: impl std::fmt::Display) {
    let message = format!("submit failed: {error}");
    for r in entry_results {
        if r.error_code == NONE {
            r.error_code = COORDINATOR_NOT_AVAILABLE;
            r.error_message = Some(message.clone());
        }
    }
}

pub(super) fn encode_whole_request_error(
    req: &AlterClientQuotasRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // `AlterClientQuotasRequest.getErrorResponse` answers one row per request
    // entry, with the entity components as the request sent them.
    let entries: Vec<RespEntry> = req
        .entries
        .iter()
        .map(|e| {
            let entity: EntityKey = e
                .entity
                .iter()
                .map(|d| (d.entity_type.clone(), d.entity_name.clone()))
                .collect();
            err_entry(&entity, code, msg.into())
        })
        .collect();
    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    super::encode_response(&resp, api_version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::codes::INVALID_REQUEST;

    #[test]
    fn entry_helpers_preserve_wire_fields() {
        let entity: EntityKey = vec![("user".into(), Some("alice".into()))];

        let ok = ok_entry(&entity);
        let expected_ok = RespEntry {
            error_code: 0,
            error_message: None,
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let err = err_entry(&entity, INVALID_REQUEST, "bad quota".into());
        let expected_err = RespEntry {
            error_code: INVALID_REQUEST,
            error_message: Some("bad quota".into()),
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);
    }

    #[test]
    fn submit_error_only_stamps_successful_entries() {
        let mut results = vec![
            ok_entry(&[("user".into(), Some("alice".into()))]),
            err_entry(
                &[("user".into(), Some("bob".into()))],
                INVALID_REQUEST,
                "invalid bob quota".into(),
            ),
        ];

        apply_submit_error(&mut results, "raft unavailable");

        let expected = vec![
            RespEntry {
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: raft unavailable".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            RespEntry {
                error_code: INVALID_REQUEST,
                error_message: Some("invalid bob quota".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("bob".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(results == expected);
    }
}
