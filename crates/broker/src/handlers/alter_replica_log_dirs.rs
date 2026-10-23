//! `AlterReplicaLogDirs` (`api_key=34`, KIP-113) moves a replica between the
//! log directories of one broker.
//!
//! This intra-broker log-directory reassignment moves a hosted replica from
//! one of the configured `log.dirs` of this broker to another, without a new
//! replication from the leader. It backs `kafka-log-dirs --alter` and the
//! `--reassignment-json-file` log-dir overrides of
//! `kafka-reassign-partitions`.
//!
//! The handler validates the inputs immediately, starts a background
//! replicator task for each move with [`crate::future_log::start_move`], and
//! returns success. The data copy and the atomic dir-rename occur in the
//! background. Clients poll `DescribeLogDirs` and watch `is_future_key`
//! change from `true` to `false` to find the end of the move.
//!
//! The handler enforces Cluster.Alter before it starts any move.

#[cfg(test)]
use crate::producer_state::ProducerState;
use {
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult},
        broker::Broker,
        codes,
        error::BrokerError,
        future_log::{self, MoveError},
        handlers::{acl_wire::CLUSTER_RESOURCE_NAME, encode_response},
        network::auth::ConnectionAuth,
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        Decode,
        owned::{
            alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
        },
    },
    krabka_security::{AuthMethod, Principal},
    std::{collections::BTreeMap, net::SocketAddr, path::PathBuf},
};

/// A partition addressed by topic name and index, ordered by topic and then index.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

pub(crate) fn handle<'a>(
    broker: &'a Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    auth: &'a ConnectionAuth,
    peer: &'a SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let all_log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let controller = broker.controller.clone();
    let move_policy = future_log::MovePolicy {
        retry_backoff: broker.config.future_log_move_retry_backoff,
        read_chunk: broker.config.future_log_move_read_chunk,
        throttle: broker.throttle_state.alter_log_dirs.clone(),
    };
    Box::pin(async move {
        let anonymous;
        let principal = if let Some(principal) = auth.principal() {
            principal
        } else {
            anonymous = Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: AuthMethod::Anonymous,
                groups: vec![],
            };
            &anonymous
        };

        let image = broker.controller.current_image();
        let authorized = broker
            .config
            .authorizer
            .authorize(&*image, &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: ResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
                operation: AclOperation::Alter,
            })
            == AuthorizationResult::Allow;

        if !authorized {
            let mut cur: &[u8] = &req_bytes;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            // Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` walks the
            // request as sent: one topic result per (directory, topic) pair,
            // in request order, with every listed partition set to the same
            // error code. It never merges or sorts entries, so a topic named
            // under two directories gets two results here too.
            let results = req
                .dirs
                .into_iter()
                .flat_map(|dir| dir.topics)
                .map(|topic| AlterReplicaLogDirTopicResult {
                    topic_name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition_index| AlterReplicaLogDirPartitionResult {
                            partition_index,
                            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            return encode_response(&resp, version);
        }

        let mut cur: &[u8] = &req_bytes;
        let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
        let image = controller.current_image();

        // The wire format lets a client list the same partition under several
        // target dirs. Kafka's `AlterReplicaLogDirsRequest.partitionDirs()`
        // collects them into a map that keeps the LAST destination, and
        // `ReplicaManager.alterReplicaLogDirs` handles each partition once, so
        // a superseded destination is neither validated nor started.
        let mut destinations: BTreeMap<TopicPartitionKey, PathBuf> = BTreeMap::new();
        for dir in req.dirs {
            let target_path = PathBuf::from(&dir.path);
            for topic in dir.topics {
                for partition_index in topic.partitions {
                    destinations.insert(
                        TopicPartitionKey {
                            topic: topic.name.clone(),
                            partition: partition_index,
                        },
                        target_path.clone(),
                    );
                }
            }
        }

        // (topic, partition) → error code.
        let mut per_partition: BTreeMap<TopicPartitionKey, i16> = BTreeMap::new();
        for (
            TopicPartitionKey {
                topic,
                partition: partition_index,
            },
            target_path,
        ) in destinations
        {
            let code = match future_log::start_move(
                &partitions,
                &future_logs,
                &all_log_dirs,
                &log_dir_status,
                &log_config,
                (&topic, krabka_ids::PartitionIndex(partition_index)),
                image.topic(&topic).map(|topic| topic.topic_id),
                &target_path,
                move_policy.clone(),
            )
            .await
            {
                Ok(()) => codes::NONE,
                Err(MoveError::TopicNameTooLong) => codes::INVALID_TOPIC_EXCEPTION,
                Err(MoveError::LogDirNotFound) => codes::LOG_DIR_NOT_FOUND,
                Err(MoveError::Cordoned) => codes::INVALID_REPLICA_ASSIGNMENT,
                Err(MoveError::ReplicaNotAvailable) => codes::REPLICA_NOT_AVAILABLE,
                Err(MoveError::Storage(error)) => {
                    tracing::warn!(
                        topic = %topic,
                        partition = partition_index,
                        target = %target_path.display(),
                        error = %error,
                        "replica log-dir move failed"
                    );
                    codes::KAFKA_STORAGE_ERROR
                }
            };
            per_partition.insert(
                TopicPartitionKey {
                    topic,
                    partition: partition_index,
                },
                code,
            );
        }

        // Group per-partition results back into the response's
        // per-topic shape.
        let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
            BTreeMap::new();
        for (TopicPartitionKey { topic, partition }, code) in per_partition {
            by_topic
                .entry(topic)
                .or_default()
                .push(AlterReplicaLogDirPartitionResult {
                    partition_index: partition,
                    error_code: code,
                    ..Default::default()
                });
        }

        let results: Vec<_> = by_topic
            .into_iter()
            .map(|(name, partitions)| AlterReplicaLogDirTopicResult {
                topic_name: name,
                partitions,
                ..Default::default()
            })
            .collect();

        let resp = AlterReplicaLogDirsResponse {
            results,
            ..Default::default()
        };
        encode_response(&resp, version)
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::spawn_partition,
            handlers::test_support as handler_fixture_support,
            log_dir::partition_dir,
            partition::Partition,
            test_support::{
                self as broker_fixture_support, DenyAll, peer, principal,
                start_broker_with_authorizer,
            },
        },
        assert2::assert,
        krabka_protocol::owned::alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, MAX_VERSION, MIN_VERSION,
        },
        krabka_security::SaslMechanism,
        std::sync::Arc,
    };

    krabka_macros::wire_codec_helpers! {
        encode_request(AlterReplicaLogDirsRequest);
        decode_response(AlterReplicaLogDirsResponse);
    }

    fn expected_partition_response(
        topic: &str,
        partition_index: i32,
        error_code: i16,
    ) -> AlterReplicaLogDirsResponse {
        krabka_macros::unthrottled_wire!(AlterReplicaLogDirsResponse {
            results: vec![krabka_macros::tagged_wire!(AlterReplicaLogDirTopicResult {
                topic_name: topic.to_string(),
                partitions: vec![krabka_macros::tagged_wire!(
                    AlterReplicaLogDirPartitionResult {
                        partition_index,
                        error_code,
                    }
                )],
            })],
        })
    }

    async fn assert_partition_response(
        broker: &Broker,
        version: i16,
        request: AlterReplicaLogDirsRequest,
        topic: &str,
        error_code: i16,
        label: &str,
    ) {
        let bytes = handle(
            broker,
            version,
            1,
            &encode_request(&request, version),
            &authed("alice"),
            &peer(),
        )
        .await
        .expect("handle");
        let expected = expected_partition_response(topic, 0, error_code);
        assert!(decode_response(&bytes, version) == expected, "{label}");
    }

    #[tokio::test]
    async fn handle_preserves_unknown_target_response_shape() {
        let version = 2;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [broker_fixture_support::start_broker_with(|_| {})];
        };
        let req = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: "/tmp/krabka-missing-log-dir".into(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".into(),
                    partitions: vec![7],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, version);

        let bytes = handle(&broker, version, 123, &req_bytes, &authed("alice"), &peer())
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = krabka_macros::unthrottled_wire!(AlterReplicaLogDirsResponse {
            results: vec![krabka_macros::tagged_wire!(AlterReplicaLogDirTopicResult {
                topic_name: "orders".to_string(),
                partitions: vec![krabka_macros::tagged_wire!(
                    AlterReplicaLogDirPartitionResult {
                        partition_index: 7,
                        error_code: codes::LOG_DIR_NOT_FOUND,
                    }
                )],
            })],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka matches the destination against the configured absolute paths by
    /// string equality (`LogManager.isLogDirOnline`) and refuses a partition
    /// whose future directory name passes 255 characters before it reads the
    /// destination (`ReplicaManager.alterReplicaLogDirs`). A destination that
    /// names a configured directory reaches the hosted-replica check, which
    /// answers `REPLICA_NOT_AVAILABLE` for a partition this broker does not
    /// host.
    // The cases spell POSIX paths and one is a symbolic link.
    #[cfg(unix)]
    #[tokio::test]
    async fn destination_and_topic_name_validation_matches_kafka() {
        let version = 2;
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = std::fs::canonicalize(extra.path()).expect("canonical extra dir");
        let links = tempfile::tempdir().expect("link dir");
        let link = links.path().join("link");
        std::os::unix::fs::symlink(&extra_dir, &link).expect("symlink to the extra dir");
        let extra_str = extra_dir.display().to_string();
        let name = extra_dir
            .file_name()
            .expect("tempdir has a name")
            .to_string_lossy()
            .to_string();
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_with_extra_log_dir(&extra_dir)];
        };

        // A topic of 213 characters gives Kafka's future directory name,
        // `<topic>-0.<32 hex>-future`, exactly 255 characters.
        let longest = "t".repeat(213);
        let too_long = "t".repeat(214);
        let cases = [
            (
                "the configured path",
                extra_str.clone(),
                "orders",
                codes::REPLICA_NOT_AVAILABLE,
            ),
            (
                "a trailing slash",
                format!("{extra_str}/"),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a trailing dot",
                format!("{extra_str}/."),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a parent hop",
                format!("{extra_str}/../{name}"),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a relative path",
                extra_str.trim_start_matches('/').to_string(),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a symbolic link",
                link.display().to_string(),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "the longest topic name",
                extra_str.clone(),
                longest.as_str(),
                codes::REPLICA_NOT_AVAILABLE,
            ),
            (
                "a topic name one too long",
                extra_str.clone(),
                too_long.as_str(),
                codes::INVALID_TOPIC_EXCEPTION,
            ),
            (
                "a too-long name before an unknown path",
                format!("{extra_str}/"),
                too_long.as_str(),
                codes::INVALID_TOPIC_EXCEPTION,
            ),
        ];
        for (label, path, topic, error_code) in cases {
            let req = AlterReplicaLogDirsRequest {
                dirs: vec![AlterReplicaLogDir {
                    path,
                    topics: vec![AlterReplicaLogDirTopic {
                        name: topic.into(),
                        partitions: vec![0],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert_partition_response(&broker, version, req, topic, error_code, label).await;
        }
        broker_handle.shutdown().await;
    }

    fn host_orders(broker: &Broker) -> Arc<Partition> {
        let primary = broker.config.log_dir.clone();
        let partition_dir = partition_dir(&primary, "orders", 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let hosted = spawn_partition(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            primary.clone(),
            krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
                .expect("open log"),
            broker.log_dir_status.clone(),
            std::sync::Arc::new(ProducerState::new()),
            false,
        );
        broker.partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            hosted.clone(),
        );
        hosted
    }

    #[tokio::test]
    async fn denial_starts_no_move_for_a_hosted_partition() {
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = extra.path().to_path_buf();
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [broker_fixture_support::start_broker_with(|config| {
                config.authorizer = Arc::new(DenyAll);
                config.extra_log_dirs = vec![extra_dir.clone()];
            })];
        };
        let hosted = host_orders(&broker);
        let req = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: extra_dir.display().to_string(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".into(),
                    partitions: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let target_entries = std::fs::read_dir(&extra_dir).expect("target dir").count();
        for auth in [authed("alice"), ConnectionAuth::Anonymous] {
            for version in MIN_VERSION..=MAX_VERSION {
                let bytes = handle(
                    &broker,
                    version,
                    1,
                    &encode_request(&req, version),
                    &auth,
                    &peer(),
                )
                .await
                .expect("handle");
                assert!(
                    decode_response(&bytes, version)
                        == expected_partition_response(
                            "orders",
                            0,
                            codes::CLUSTER_AUTHORIZATION_FAILED
                        )
                );
                assert!(broker.future_logs.is_empty());
                assert!(
                    std::fs::read_dir(&extra_dir).expect("target dir").count() == target_entries
                );
                assert!(hosted.log_dir.load_full().as_path() == broker.config.log_dir.as_path());
            }
        }
        broker_handle.shutdown().await;
    }

    /// Kafka collects the request into `partitionDirs()`, a map that keeps the
    /// LAST destination of a partition listed twice, and handles each
    /// partition once. A superseded destination neither answers nor starts a
    /// move, whatever the last one does.
    #[tokio::test]
    async fn a_partition_listed_twice_moves_to_its_last_destination_only() {
        let version = 2;
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = extra.path().to_path_buf();
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_with_extra_log_dir(&extra_dir)];
        };
        let hosted = host_orders(&broker);
        let valid = extra_dir.display().to_string();
        let unknown = "/krabka-missing-log-dir".to_string();

        // (name, destinations in request order, expected code, whether the
        // partition ends up in the extra directory)
        let cases = [
            (
                "a valid destination superseded by an unknown one",
                [valid.clone(), unknown.clone()],
                codes::LOG_DIR_NOT_FOUND,
                false,
            ),
            (
                "an unknown destination superseded by a valid one",
                [unknown, valid],
                codes::NONE,
                true,
            ),
        ];
        for (name, destinations, error_code, moves) in cases {
            let req = AlterReplicaLogDirsRequest {
                dirs: destinations
                    .into_iter()
                    .map(|path| AlterReplicaLogDir {
                        path,
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };

            assert_partition_response(&broker, version, req, "orders", error_code, name).await;
            // An empty partition moves within milliseconds once a move starts,
            // so a move that never starts is given a short look.
            let wait = if moves {
                std::time::Duration::from_secs(20)
            } else {
                std::time::Duration::from_millis(500)
            };
            let deadline = tokio::time::Instant::now() + wait;
            let mut in_extra_dir = false;
            while !in_extra_dir && tokio::time::Instant::now() < deadline {
                in_extra_dir = std::fs::canonicalize(hosted.log_dir.load_full().as_path()).ok()
                    == std::fs::canonicalize(&extra_dir).ok();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(in_extra_dir == moves, "{name}");
        }
        broker_handle.shutdown().await;
    }
    fn authed(name: &str) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: principal(name),
            mechanism: SaslMechanism::Plain,
            expires_at_ms: None,
            authenticated_via_token: false,
        }
    }

    fn topic_result(name: &str, partitions: &[i32]) -> AlterReplicaLogDirTopicResult {
        AlterReplicaLogDirTopicResult {
            topic_name: name.to_string(),
            partitions: partitions
                .iter()
                .map(|&partition_index| AlterReplicaLogDirPartitionResult {
                    partition_index,
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` produces one
    /// topic result per (directory, topic) pair, in request order, never
    /// merging or sorting them by topic name. A denied `Cluster.Alter` must
    /// match that shape exactly, including when the same topic is named
    /// under two directories or when topics are listed out of name order.
    #[tokio::test]
    async fn denial_emits_one_row_per_dir_and_topic_in_request_order() {
        let version = 2;
        for (case, dirs, expected_results) in [
            (
                "one dir, one topic",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![AlterReplicaLogDirTopic {
                        name: "orders".into(),
                        partitions: vec![1, 0, 1],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                vec![topic_result("orders", &[1, 0, 1])],
            ),
            (
                "same topic under two dirs stays two rows",
                vec![
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data0".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data1".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                vec![topic_result("orders", &[0]), topic_result("orders", &[1])],
            ),
            (
                "topics in reverse name order are not sorted",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![
                        AlterReplicaLogDirTopic {
                            name: "widgets".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                        AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                vec![topic_result("widgets", &[0]), topic_result("orders", &[0])],
            ),
        ] {
            krabka_macros::broker_test_setup! {
                bind(broker_handle, _dir, broker);
                boot [start_broker_with_authorizer(Arc::new(DenyAll))];
            };
            let peer = peer();
            let req = AlterReplicaLogDirsRequest {
                dirs,
                ..Default::default()
            };
            let body = encode_request(&req, version);

            let expected = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results: expected_results,
                ..Default::default()
            };
            for auth in [authed("alice"), ConnectionAuth::Anonymous] {
                let bytes = handle(&broker, version, 0, &body, &auth, &peer)
                    .await
                    .expect("handle");
                let resp = decode_response(&bytes, version);
                assert2::check!(resp == expected, "case {case}");
            }
            broker_handle.shutdown().await;
        }
    }
}
