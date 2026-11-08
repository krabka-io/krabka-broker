//! Item generators for the broker's own fixtures.

use {
    crate::meta::{argument_array, mode},
    moxy::{ast::ParseError, token::TokenStream},
};

#[path = "../../macros/src/shared/fixtures.rs"]
mod shared;

pub use shared::*;

/// Generate the first matching header lookup with the caller's UTF-8 policy.
pub fn record_header_text(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let [name_tokens, mode_tokens] = argument_array(tokens)?;
    let name = name(name_tokens)?;
    let strict = mode(mode_tokens, [("strict", true), ("lossy", false)])?;
    Ok(moxy::template! {
        fn {{ name }}(record: &::krabka_protocol::records::Record, key: &str) -> Option<String> {
            record.headers.iter()
                .find(|header| header.key == key)
                .and_then(|header| header.value.as_ref())
                .map(|value| {
                    @if strict { String::from_utf8(value.to_vec()).unwrap() }
                    @else { String::from_utf8_lossy(value).into_owned() }
                })
        }
    })
}

/// Generate the three DLQ counter reads with explicit caller types and docs.
pub fn share_dlq_meters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let [name_tokens, metrics, label] = argument_array(tokens)?;
    let mut name_tokens = Vec::from(name_tokens);
    let name = name(name_tokens.pop().into_iter().collect())?;
    let attributes = TokenStream::from(name_tokens);
    Ok(moxy::template! {
        {{ attributes }}
        fn {{ name }}(metrics: &{{ metrics }}, group: &str) -> (u64, u64, u64) {
            let label = {{ label }} { group_id: group.to_owned() };
            (
                metrics.share_group_dlq_records.get_or_create(&label).get(),
                metrics.share_group_dlq_produce_requests.get_or_create(&label).get(),
                metrics.share_group_dlq_failed_produce_requests.get_or_create(&label).get(),
            )
        }
    })
}

pub fn delete_topic_request(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        /// Delete one named topic using both legacy and flexible request fields.
        fn {{ name }}(topic: &str) -> ::krabka_protocol::owned::delete_topics_request::DeleteTopicsRequest {
            ::krabka_protocol::owned::delete_topics_request::DeleteTopicsRequest {
                topics: ::std::vec![::krabka_protocol::owned::delete_topics_request::DeleteTopicState {
                    name: Some(topic.into()),
                    ..Default::default()
                }],
                topic_names: ::std::vec![topic.into()],
                timeout_ms: 5_000,
                ..Default::default()
            }
        }
    })
}
