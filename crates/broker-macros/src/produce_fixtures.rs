//! A single-partition Produce request shared by wire-test fixtures.

use {
    crate::fixtures::named_items,
    moxy::{ast::ParseError, token::TokenStream},
};

pub fn single_partition_produce(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
            #[derive(Clone, Copy)]
            pub(crate) struct WireAcknowledgements(pub i16);
            #[derive(Clone, Copy, Default)]
            pub(crate) enum ProduceAcknowledgements {
                NoResponse,
                #[default]
                Leader,
                AllReplicas,
                Unsupported(WireAcknowledgements),
            }
            impl ProduceAcknowledgements {
                /// Preserve observed and deliberately malformed acknowledgement codes.
                pub(crate) fn from_wire(code: WireAcknowledgements) -> Self {
                    match code.0 { 0 => Self::NoResponse, 1 => Self::Leader, -1 => Self::AllReplicas, _ => Self::Unsupported(code) }
                }
                fn wire_code(self) -> i16 {
                    match self { Self::NoResponse => 0, Self::Leader => 1, Self::AllReplicas => -1, Self::Unsupported(code) => code.0 }
                }
            }
            #[derive(Clone, Copy, ::derive_more::From)]
            pub(crate) struct ProduceTimeoutMillis(pub i32);
            impl Default for ProduceTimeoutMillis {
                fn default() -> Self { Self(5_000) }
            }
            /// A Produce request for one partition of one topic.
            #[bon::builder]
            pub(crate) fn {{ name }}(
                #[builder(into)] records: Option<::krabka_protocol::records::RecordsPayload>,
                #[builder(into, default = "orders".to_owned())] topic: String,
                #[builder(default)] topic_id: ::krabka_protocol::primitives::uuid::Uuid,
                #[builder(into, default)] partition: ::krabka_ids::PartitionIndex,
                #[builder(default)] acknowledgements: ProduceAcknowledgements,
                #[builder(into, default)] timeout: ProduceTimeoutMillis,
            ) -> ::krabka_protocol::owned::produce_request::ProduceRequest {
                ::krabka_protocol::owned::produce_request::ProduceRequest {
                    acks: acknowledgements.wire_code(),
                    timeout_ms: timeout.0,
                    topic_data: vec![::krabka_protocol::owned::produce_request::TopicProduceData {
                        name: topic,
                        topic_id,
                        partition_data: vec![::krabka_protocol::owned::produce_request::PartitionProduceData {
                            index: partition.0,
                            records,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            }
        }
    })
}
