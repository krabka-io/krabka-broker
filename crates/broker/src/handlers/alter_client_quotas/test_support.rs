//! The request builders that the `AlterClientQuotas` tests share.
//!
//! The entry-validation tests and the live-broker tests build the same entry
//! and request shapes, so the two builders live in one module rather than
//! being duplicated per test file.

use krabka_protocol::owned::alter_client_quotas_request::{
    AlterClientQuotasRequest, EntityData, EntryData, OpData,
};

pub(super) fn entry(entity: Vec<(&str, Option<&str>)>, ops: Vec<(&str, f64, bool)>) -> EntryData {
    EntryData {
        entity: entity
            .into_iter()
            .map(|(t, n)| EntityData {
                entity_type: t.into(),
                entity_name: n.map(Into::into),
                ..Default::default()
            })
            .collect(),
        ops: ops
            .into_iter()
            .map(|(k, v, r)| OpData {
                key: k.into(),
                value: v,
                remove: r,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn request(entries: Vec<EntryData>, validate_only: bool) -> AlterClientQuotasRequest {
    AlterClientQuotasRequest {
        entries,
        validate_only,
        ..Default::default()
    }
}
