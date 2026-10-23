use {
    super::*,
    crate::{
        network::{request::parse_request, test_support::raw_request_frame},
        test_support::{broker_registration, scripted_response_body},
    },
    assert2::assert,
    krabka_client_core::{MockBroker, MockReply},
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_metadata::{BrokerRegistrationRecord, MetadataRecord},
    krabka_protocol::{
        Decode as _,
        owned::{
            add_partitions_to_txn_request,
            add_partitions_to_txn_response::{
                AddPartitionsToTxnResponse, AddPartitionsToTxnResult,
            },
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            common::add_partitions_to_txn_response::{
                add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
                add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
            },
        },
    },
    std::sync::Mutex as StdMutex,
};

const LEADER: NodeId = NodeId(2);

type RecordedRequests = Arc<StdMutex<Vec<(i16, AddPartitionsToTxnRequest)>>>;

struct Listener {
    broker: MockBroker,
    handshakes: Arc<StdMutex<usize>>,
    requests: RecordedRequests,
}

impl Listener {
    async fn start(answer: AddPartitionsToTxnResponse, close_first: bool) -> Self {
        Self::start_with_version(answer, close_first, 4).await
    }

    async fn start_with_version(
        answer: AddPartitionsToTxnResponse,
        close_first: bool,
        max_version: i16,
    ) -> Self {
        let handshakes = Arc::new(StdMutex::new(0));
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let observed_handshakes = Arc::clone(&handshakes);
        let observed_requests = Arc::clone(&requests);
        let broker = MockBroker::start_with_replies(move |key, version, correlation_id, body| {
            if key == api_versions_request::API_KEY {
                *observed_handshakes.lock().unwrap() += 1;
                let response = ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            min_version: 0,
                            max_version: api_versions_request::MAX_VERSION,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: add_partitions_to_txn_request::API_KEY,
                            min_version: 0,
                            max_version,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                return MockReply::Respond(scripted_response_body(&response, version, false));
            }
            assert!(key == add_partitions_to_txn_request::API_KEY);
            let frame = raw_request_frame(key, version, correlation_id, body);
            let parsed = parse_request(&frame, |_, version| {
                add_partitions_to_txn_request::is_flexible(version)
            })
            .unwrap();
            assert!(parsed.client_id == Some("krabka-broker-txn-1"));
            let mut body = parsed.body;
            let request = AddPartitionsToTxnRequest::decode(&mut body, version).unwrap();
            let mut requests = observed_requests.lock().unwrap();
            requests.push((version, request));
            if close_first && requests.len() == 1 {
                return MockReply::Close;
            }
            MockReply::Respond(scripted_response_body(&answer, version, true))
        })
        .await;
        Self {
            broker,
            handshakes,
            requests,
        }
    }

    fn image(&self) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: self.broker.addr.ip().to_string(),
                port: self.broker.addr.port(),
                ..broker_registration(LEADER)
            },
        ));
        image
    }

    fn handshakes(&self) -> usize { *self.handshakes.lock().unwrap() }
}

fn client() -> VerificationClient {
    VerificationClient::new(
        NodeId(1),
        Arc::new(InterBrokerClient::new(None, None)),
        ListenerProtocol::Plaintext,
        "PLAINTEXT".into(),
        "localhost".into(),
    )
}

fn partition(topic: &str, index: i32) -> TopicPartition {
    TopicPartition {
        topic: topic.into(),
        partition: PartitionIndex(index),
    }
}

fn check() -> TransactionCheck<'static> {
    TransactionCheck {
        transactional_id: "tid",
        producer_id: ProducerId(7),
        producer_epoch: 3,
        partitions: vec![partition("b", 2), partition("a", 0), partition("b", 1)],
        verify_only: true,
    }
}

fn response() -> AddPartitionsToTxnResponse {
    AddPartitionsToTxnResponse {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: "tid".into(),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: "b".into(),
                results_by_partition: vec![
                    AddPartitionsToTxnPartitionResult {
                        partition_index: 1,
                        partition_error_code: codes::PRODUCER_FENCED,
                        ..Default::default()
                    },
                    AddPartitionsToTxnPartitionResult {
                        partition_index: 2,
                        partition_error_code: codes::NONE,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn preserves_wire_request_and_partition_answer_order_on_a_reused_connection() {
    let listener = Listener::start(response(), false).await;
    let client = client();
    let image = listener.image();
    for verify_only in [true, false] {
        let check = TransactionCheck {
            verify_only,
            ..check()
        };
        let answers = client.add_or_verify(&image, LEADER, &check).await;
        assert!(
            answers
                == Ok(vec![
                    (partition("b", 2), codes::NONE),
                    (partition("a", 0), codes::UNKNOWN_SERVER_ERROR),
                    (partition("b", 1), codes::PRODUCER_FENCED),
                ])
        );
    }
    assert!(listener.handshakes() == 1);
    let topics = expected_topics();
    let expected: Vec<_> = [true, false]
        .into_iter()
        .map(|verify_only| {
            (4, AddPartitionsToTxnRequest {
                transactions: vec![AddPartitionsToTxnTransaction {
                    transactional_id: "tid".into(),
                    producer_id: 7,
                    producer_epoch: 3,
                    topics: topics.clone(),
                    verify_only,
                    ..Default::default()
                }],
                ..Default::default()
            })
        })
        .collect();
    assert!(*listener.requests.lock().unwrap() == expected);
}

fn expected_topics() -> Vec<AddPartitionsToTxnTopic> {
    vec![
        AddPartitionsToTxnTopic {
            name: "b".into(),
            partitions: vec![2, 1],
            ..Default::default()
        },
        AddPartitionsToTxnTopic {
            name: "a".into(),
            partitions: vec![0],
            ..Default::default()
        },
    ]
}

#[tokio::test]
async fn endpoint_changes_and_closed_connections_redial_and_close_the_stale_connection() {
    let first = Listener::start(response(), false).await;
    let second = Listener::start(response(), false).await;
    let client = client();
    let first_port = first.broker.addr.port();
    let first_connection = client
        .verification_connection(LEADER, "127.0.0.1", first_port)
        .await
        .unwrap();
    client
        .verification_connection(LEADER, "127.0.0.1", first_port)
        .await
        .unwrap();
    assert!(first.handshakes() == 1);
    let second_connection = client
        .verification_connection(LEADER, "127.0.0.1", second.broker.addr.port())
        .await
        .unwrap();
    assert!(first_connection.is_closed());
    assert!(second.handshakes() == 1);
    let renamed = client
        .verification_connection(LEADER, "localhost", second.broker.addr.port())
        .await
        .unwrap();
    assert!(second_connection.is_closed());
    assert!(second.handshakes() == 2);
    renamed.close();
    client
        .verification_connection(LEADER, "localhost", second.broker.addr.port())
        .await
        .unwrap();
    assert!(second.handshakes() == 3);
    client.drop_verification_connection(LEADER).await;
    assert!(client.connections.lock().await.is_empty());
}

#[tokio::test]
async fn a_failed_send_invalidates_the_cache_and_the_next_call_redials() {
    let listener = Listener::start(response(), true).await;
    let client = client();
    assert!(
        client
            .add_or_verify(&listener.image(), LEADER, &check())
            .await
            == Err(codes::NETWORK_EXCEPTION)
    );
    assert!(client.connections.lock().await.is_empty());
    assert!(
        client
            .add_or_verify(&listener.image(), LEADER, &check())
            .await
            .is_ok()
    );
    assert!(listener.handshakes() == 2);
}

#[tokio::test]
async fn top_level_errors_precede_partition_answers_and_missing_brokers_do_not_dial() {
    let listener = Listener::start(
        AddPartitionsToTxnResponse {
            error_code: codes::NOT_COORDINATOR,
            ..response()
        },
        false,
    )
    .await;
    let client = client();
    assert!(
        client
            .add_or_verify(&listener.image(), LEADER, &check())
            .await
            == Err(codes::NOT_COORDINATOR)
    );
    assert!(
        client
            .add_or_verify(&MetadataImage::new(uuid::Uuid::nil()), LEADER, &check())
            .await
            == Err(codes::COORDINATOR_NOT_AVAILABLE)
    );
    assert!(listener.handshakes() == 1);
}

#[tokio::test]
async fn negotiated_legacy_versions_keep_the_legacy_request_fields() {
    let listener =
        Listener::start_with_version(AddPartitionsToTxnResponse::default(), false, 3).await;
    let client = client();
    let check = check();
    assert!(
        client
            .add_or_verify(&listener.image(), LEADER, &check)
            .await
            == Ok(check
                .partitions
                .iter()
                .cloned()
                .map(|partition| (partition, codes::UNKNOWN_SERVER_ERROR))
                .collect())
    );
    let expected = AddPartitionsToTxnRequest {
        v3_and_below_transactional_id: "tid".into(),
        v3_and_below_producer_id: 7,
        v3_and_below_producer_epoch: 3,
        v3_and_below_topics: expected_topics(),
        ..Default::default()
    };
    assert!(*listener.requests.lock().unwrap() == vec![(3, expected)]);
}
