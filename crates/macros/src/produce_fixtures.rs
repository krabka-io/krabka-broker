//! A single-partition Produce request shared by wire-test fixtures.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn single_partition_produce(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(
            name: impl Into<String>,
            topic_id: ::krabka_protocol::primitives::uuid::Uuid,
            index: i32,
            records: Option<::krabka_protocol::records::RecordsPayload>,
            (acks, timeout_ms): (i16, i32),
        ) -> ::krabka_protocol::owned::produce_request::ProduceRequest {
            ::krabka_protocol::owned::produce_request::ProduceRequest {
                acks,
                timeout_ms,
                topic_data: vec![::krabka_protocol::owned::produce_request::TopicProduceData {
                    name: name.into(),
                    topic_id,
                    partition_data: vec![::krabka_protocol::owned::produce_request::PartitionProduceData {
                        index,
                        records,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    })
}
