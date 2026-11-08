//! Wire and metadata fixtures shared across test crates.

use {
    crate::{
        fixtures::{function, name, named_items},
        meta::item_body,
    },
    moxy::{
        ast::ParseError,
        token::{Span, TokenStream, TokenTree},
    },
};

pub fn share_produce_parameters(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    if let Some(token) = meta.into_iter().next() {
        return Err(ParseError::new(token.span(), "expected no arguments"));
    }
    let mut tokens = item_body(item, "share_produce_parameters", TokenTree::is_keyword_fn)?.tokens;
    let function = tokens
        .iter()
        .position(TokenTree::is_keyword_fn)
        .ok_or_else(|| ParseError::new(Span::call_site(), "expected a function"))?;
    let Some(TokenTree::Group(arguments)) = tokens.get_mut(function + 2) else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected nongeneric function parameters",
        ));
    };
    if !arguments.delim.is_paren() || arguments.tokens.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "expected nonempty function parameters",
        ));
    }
    if !arguments
        .tokens
        .iter()
        .last()
        .is_some_and(TokenTree::is_punct_comma)
    {
        arguments.tokens.extend(moxy::template! { , });
    }
    arguments.tokens.extend(moxy::template! {
        #[builder(default = "t")] topic: &'_ str,
        #[builder(default)] topic_id: ::uuid::Uuid,
        #[builder(into, default)] partition: ::krabka_ids::PartitionIndex,
    });
    Ok(tokens.into())
}

pub fn refused_fetch_partition(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        /// Kafka's `FetchResponse.partitionResponse` row for a partition the
        /// fetch refused with `error_code`.
        fn {{ name }}(
            partition_index: i32,
            error_code: i16,
        ) -> ::krabka_protocol::owned::fetch_response::PartitionData {
            ::krabka_protocol::owned::fetch_response::PartitionData {
                partition_index,
                error_code,
                high_watermark: -1,
                last_stable_offset: -1,
                log_start_offset: -1,
                aborted_transactions: ::std::option::Option::Some(::std::vec::Vec::new()),
                preferred_read_replica: -1,
                records: ::std::option::Option::Some(
                    ::krabka_protocol::records::RecordsPayload::Legacy(::bytes::Bytes::new()),
                ),
                ..::std::default::Default::default()
            }
        }
    })
}

pub fn create_topic(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::Display, ::derive_more::From, ::derive_more::Into)]
            pub(crate) struct TopicPartitionCount(pub i32);
            impl Default for TopicPartitionCount {
                fn default() -> Self { Self(1) }
            }
            #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::Display, ::derive_more::From, ::derive_more::Into)]
            pub(crate) struct TopicReplicationFactor(pub i16);
            impl Default for TopicReplicationFactor {
                fn default() -> Self { Self(1) }
            }
            #[derive(Clone, Copy)]
            pub(crate) struct CreateTopicSetup<'a> {
                pub topic: &'a str,
                pub configs: &'a [(&'a str, &'a str)],
                pub num_partitions: TopicPartitionCount,
                pub replication_factor: TopicReplicationFactor,
                pub timeout: ::krabka_units::Time,
            }
            impl Default for CreateTopicSetup<'_> {
                fn default() -> Self {
                    Self { topic: "orders", configs: &[], num_partitions: TopicPartitionCount(1), replication_factor: TopicReplicationFactor(1), timeout: ::krabka_units::millis(5_000) }
                }
            }
            #[bon::builder]
            pub(crate) fn {{ name }}(
                #[builder(default = "orders")] topic: &'_ str,
                #[builder(default = &[])] configs: &'_ [(&'_ str, &'_ str)],
                #[builder(into, default = TopicPartitionCount(1))] num_partitions: TopicPartitionCount,
                #[builder(into, default = TopicReplicationFactor(1))] replication_factor: TopicReplicationFactor,
                #[builder(default = ::krabka_units::millis(5_000))] timeout: ::krabka_units::Time,
            ) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                use ::krabka_units::convert::TimeExt as _;
                ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                    topics: vec![::krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: topic.to_owned(), num_partitions: num_partitions.0, replication_factor: replication_factor.0,
                        configs: configs.iter().map(|(name, value)| ::krabka_protocol::owned::create_topics_request::CreatableTopicConfig {
                            name: (*name).to_owned(), value: Some((*value).to_owned()), ..Default::default()
                        }).collect(),
                        ..Default::default()
                    }], timeout_ms: i32::try_from(timeout.millis_i64()).expect("fixture timeout fits Kafka field"), ..Default::default()
                }
            }
            pub(crate) fn configured_topic_request_from(
                setup: CreateTopicSetup<'_>,
            ) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                configured_topic_request()
                    .topic(setup.topic)
                    .configs(setup.configs)
                    .num_partitions(setup.num_partitions.0)
                    .replication_factor(setup.replication_factor.0)
                    .timeout(setup.timeout)
                    .call()
            }
        }
    })
}

pub fn consumer_fetch(input: TokenStream) -> Result<TokenStream, ParseError> {
    function(
        input,
        &moxy::template! { pub(crate) },
        &moxy::template! { topic: &str },
        &moxy::template! { ::krabka_protocol::owned::fetch_request::FetchRequest },
        &moxy::template! {
            ::krabka_protocol::owned::fetch_request::FetchRequest {
                replica_id: -1, max_wait_ms: 0, min_bytes: 1, max_bytes: 1 << 20,
                topics: vec![::krabka_protocol::owned::fetch_request::FetchTopic {
                    topic: topic.to_string(), partitions: vec![::krabka_protocol::owned::fetch_request::FetchPartition {
                        partition: 0, fetch_offset: 0, partition_max_bytes: 1 << 20, ..Default::default()
                    }], ..Default::default()
                }], ..Default::default()
            }
        },
    )
}

#[cfg(test)]
mod fixture_tests {
    use {super::share_produce_parameters, assert2::check, moxy::ast::ItemFn};

    #[yare::parameterized(
        trailing_comma = { "pub async fn fixture(client: &Client, values: Vec<Bytes>,) {}" },
        no_trailing_comma = { "pub async fn fixture(client: &Client, values: Vec<Bytes>) {}" },
    )]
    fn produce_parameters_preserve_the_function_and_append_three_setters(declaration: &str) {
        let expanded =
            share_produce_parameters("".parse().unwrap(), declaration.parse().unwrap()).unwrap();
        let function = moxy::parse!({ expanded } as ItemFn).unwrap();
        check!(function.sig.ident == "fixture");
        check!(function.sig.asyncness.is_some());
        check!(function.sig.params.inner.inputs.len() == 5);
    }

    #[yare::parameterized(
        options = { "unexpected", "fn fixture(client: &Client,) {}" },
        wrong_item = { "", "struct Fixture {}" },
        generic_function = { "", "fn fixture<T>(client: &T,) {}" },
        empty_parameters = { "", "fn fixture() {}" },
    )]
    fn rejects_unsupported_signatures(options: &str, declaration: &str) {
        check!(
            share_produce_parameters(options.parse().unwrap(), declaration.parse().unwrap())
                .is_err()
        );
    }
}
