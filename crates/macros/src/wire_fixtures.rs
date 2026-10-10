//! Wire and metadata fixtures shared across test crates.

use moxy::{
    ast::ParseError,
    token::{Span, TokenStream, TokenTree},
};

pub(crate) fn topic_record(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(tokens)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(name: &str, topic_id: ::uuid::Uuid) -> ::krabka_metadata::TopicRecord {
            ::krabka_metadata::TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }
        }
    })
}

pub(crate) fn producer_batch(input: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments: Vec<_> = input.into_iter().collect();
    let (name, value) = match arguments.as_slice() {
        [TokenTree::Ident(name), comma, value @ ..]
            if comma.is_punct_comma() && !value.is_empty() =>
        {
            (name, TokenStream::from(value))
        }
        _ => {
            return Err(ParseError::new(
                Span::call_site(),
                "expected function name, record value expression",
            ));
        }
    };
    Ok(moxy::template! {
        #[derive(Clone, Copy)]
        pub(crate) struct ProducerBatchSetup {
            pub producer: (i64, i16),
            pub base_sequence: i32,
            pub records: i32,
            pub max_timestamp: i64,
            pub transactional: bool,
        }
        impl Default for ProducerBatchSetup {
            fn default() -> Self {
                Self { producer: (7, 0), base_sequence: 0, records: 1, max_timestamp: 1_000, transactional: false }
            }
        }
        pub(crate) fn {{ name }}(setup: ProducerBatchSetup) -> ::krabka_protocol::records::RecordBatch {
            let ProducerBatchSetup { producer: (producer_id, producer_epoch), base_sequence, records, max_timestamp, transactional } = setup;
            ::krabka_protocol::records::RecordBatch {
                attributes: ::krabka_protocol::records::Attributes::default().with_transactional(transactional),
                last_offset_delta: records - 1, base_timestamp: max_timestamp, max_timestamp,
                producer_id, producer_epoch, base_sequence,
                records: (0..records).map(|offset_delta| ::krabka_protocol::records::Record {
                    offset_delta, value: Some({{ value }}), ..Default::default()
                }).collect(),
                ..Default::default()
            }
        }
    })
}

pub(crate) fn create_topic(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
            #[derive(Clone, Copy)]
            pub(crate) struct CreateTopicSetup<'a> {
                pub topic: &'a str,
                pub configs: &'a [(&'a str, &'a str)],
                pub num_partitions: i32,
                pub replication_factor: i16,
                pub timeout_ms: i32,
            }
            impl Default for CreateTopicSetup<'_> {
                fn default() -> Self {
                    Self { topic: "orders", configs: &[], num_partitions: 1, replication_factor: 1, timeout_ms: 5_000 }
                }
            }
            pub(crate) fn {{ name }}(setup: CreateTopicSetup<'_>) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                let CreateTopicSetup { topic, configs, num_partitions, replication_factor, timeout_ms } = setup;
                ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                    topics: vec![::krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: topic.to_owned(), num_partitions, replication_factor,
                        configs: configs.iter().map(|(name, value)| ::krabka_protocol::owned::create_topics_request::CreatableTopicConfig {
                            name: (*name).to_owned(), value: Some((*value).to_owned()), ..Default::default()
                        }).collect(),
                        ..Default::default()
                    }], timeout_ms, ..Default::default()
                }
            }
        }
    })
}

pub(crate) fn consumer_fetch(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::function(
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

pub(crate) fn single_replica_partition(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::function(
        input,
        &moxy::template! { pub(crate) },
        &moxy::template! { topic: &str, partition: i32, node: ::krabka_metadata::NodeId, },
        &moxy::template! { ::krabka_metadata::PartitionRecord },
        &moxy::template! {
            ::krabka_metadata::PartitionRecord {
                topic: topic.to_owned(), partition, leader: node, replicas: vec![node], isr: vec![node],
                leader_epoch: ::krabka_metadata::LeaderEpoch(0), adding_replicas: vec![],
                removing_replicas: vec![], directories: vec![], partition_epoch: 0,
            }
        },
    )
}

/// The version-zero Kafka control-record key and value fixture bytes.
pub(crate) fn control_marker(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(tokens)?;
    Ok(moxy::template! {
        mod {{ name }} {
            fn versioned(payload: &[u8]) -> ::bytes::Bytes {
                let mut bytes = Vec::with_capacity(2 + payload.len());
                bytes.extend_from_slice(&0i16.to_be_bytes());
                bytes.extend_from_slice(payload);
                ::bytes::Bytes::from(bytes)
            }
            pub fn control_key(marker_type: i16) -> ::bytes::Bytes {
                versioned(&marker_type.to_be_bytes())
            }
            pub fn control_value(coordinator_epoch: i32) -> ::bytes::Bytes {
                versioned(&coordinator_epoch.to_be_bytes())
            }
        }
        pub use {{ name }}::{control_key, control_value};
    })
}
