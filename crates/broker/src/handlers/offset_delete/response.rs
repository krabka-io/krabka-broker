//! The group-level `OffsetDelete` response.
//!
//! A group-level refusal carries only its top-level code. For the group ACL
//! denial, Kafka sends `OffsetDeleteRequest.getErrorResponse`; for a
//! coordinator error (routing, a missing group, `NON_EMPTY_GROUP`, a failed
//! tombstone write), the coordinator's `OffsetDeleteResponseData` holds only
//! the code, and `OffsetDeleteResponse.Builder.merge` lets it replace every
//! row the broker had built.

use krabka_protocol::owned::offset_delete_response::OffsetDeleteResponse;

pub(super) fn whole_error(code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::codes;

    #[test]
    fn whole_error_carries_only_the_top_level_code() {
        for code in [
            codes::GROUP_AUTHORIZATION_FAILED,
            codes::NOT_COORDINATOR,
            codes::GROUP_ID_NOT_FOUND,
        ] {
            let expected = OffsetDeleteResponse {
                error_code: code,
                throttle_time_ms: 0,
                topics: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            };
            check!(whole_error(code) == expected);
        }
    }
}
