//! Resolving a `DeleteTopics` request into the list of topics the handler
//! will act on, across the two request shapes the protocol has carried.
//!
//! v0-5 sends `topic_names` and knows nothing about topic ids. v6+ sends
//! `topics`, where KIP-516 lets a client identify a topic by UUID alone.
//! Whether a row was requested by id decides which error code a miss reports,
//! so that flag travels alongside the resolved name.

use krabka_protocol::{
    owned::delete_topics_request::DeleteTopicsRequest, primitives::uuid::Uuid as WireUuid,
};

/// One requested topic: the name resolved from the metadata image (`None` when
/// the image does not know it), whether the client identified the topic by id,
/// and the topic id the client sent.
pub(super) type TopicNameRequest = (Option<String>, bool, WireUuid);

/// Reports whether the client identified this topic by id rather than by name.
///
/// KIP-516: an id-based request that misses returns `UNKNOWN_TOPIC_ID` instead
/// of `UNKNOWN_TOPIC_OR_PARTITION`.
pub(super) fn requested_by_topic_id(name: Option<&String>, id: WireUuid) -> bool {
    name.is_none_or(std::string::String::is_empty) && id != WireUuid::ZERO
}

/// Collects `(resolved_name, requested_by_id, requested_topic_id)` for every
/// topic in the request.
///
/// When the client sent only a topic id, the name is resolved from the current
/// image and the entry is marked id-based so that a miss returns
/// `UNKNOWN_TOPIC_ID` (KIP-516) rather than `UNKNOWN_TOPIC_OR_PARTITION`.
pub(super) fn resolve_topic_names(
    request: &DeleteTopicsRequest,
    image: &krabka_metadata::MetadataImage,
) -> Vec<TopicNameRequest> {
    if !request.topic_names.is_empty() {
        return request
            .topic_names
            .iter()
            .map(|name| (Some(name.clone()), false, WireUuid::ZERO))
            .collect();
    }
    request
        .topics
        .iter()
        .map(|state| {
            let requested_by_id = requested_by_topic_id(state.name.as_ref(), state.topic_id);
            let name = if requested_by_id {
                image
                    .topic_by_id(&uuid::Uuid::from_bytes(state.topic_id.0))
                    .map(|topic| topic.name.clone())
            } else {
                state.name.clone()
            };
            (name, requested_by_id, state.topic_id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn requested_by_topic_id_requires_empty_name_and_nonzero_id() {
        let id = WireUuid([7; 16]);
        let empty = String::new();
        let named = String::from("orders");

        check!(requested_by_topic_id(None, id));
        check!(requested_by_topic_id(Some(&empty), id));
        check!(!requested_by_topic_id(Some(&named), id));
        check!(!requested_by_topic_id(None, WireUuid::ZERO));
    }
}
