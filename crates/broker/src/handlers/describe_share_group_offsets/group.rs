//! One requested group's row in a `DescribeShareGroupOffsets` response, from
//! the `Describe` ACL gate to the share-state persister lookup.
//!
//! KIP-932 gives the response no top-level error code, so every refusal is a
//! per-group `error_code` and an empty topic list. That is what this module
//! decides: authorization, coordinator routing, and whether a persister is
//! installed at all. Once those hold, it hands the group's topics to `rows`.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    describe_share_group_offsets_request::{
        DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
    },
    describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup,
};

use super::{
    end_offsets::NetworkLatestOffsets,
    rows::{describe_topics, unauthorized_topic},
    topics::initialized_topics,
};
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::GroupCoordinator,
};

/// Resolve one requested group into its response row.
// cargo-mutants: share-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_group(
    broker: &Broker,
    ng: Option<&GroupCoordinator>,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    group: DescribeShareGroupOffsetsRequestGroup,
) -> DescribeShareGroupOffsetsResponseGroup {
    let gid = group.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Describe` check. On Deny → group `error_code = 30`.
    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        ResourceType::Group,
        gid.as_str(),
        AclOperation::Describe,
    ) {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            ..Default::default()
        };
    }
    // KIP-932: an explicit, empty topic list asks for nothing. Kafka's
    // `describeShareGroupOffsetsForGroup` answers with the group id and an
    // empty topic list without ever dispatching to the coordinator, so this
    // never touches the persister -- and never the coordinator-routing check
    // either: there is no coordinator work to route, so the answer must not
    // depend on which broker in the cluster happens to receive it.
    if matches!(&group.topics, Some(topics) if topics.is_empty()) {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code: codes::NONE,
            ..Default::default()
        };
    }

    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code,
            ..Default::default()
        };
    }

    // The persister is required to read SPSO. Absent (share groups
    // disabled / not yet bootstrapped) → coordinator-not-available.
    let Some(persister) = ng.and_then(|ng| ng.share_persister().cloned()) else {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code: codes::COORDINATOR_NOT_AVAILABLE,
            ..Default::default()
        };
    };

    // `group.topics` is `None` for a fetch-all request and `Some(non_empty)`
    // for an explicit list (the `Some(empty)` shape already returned above).
    // Kafka's `describeShareGroupOffsetsForGroup` partitions an explicit list
    // by topic `Describe` and appends the unauthorized topics after the
    // coordinator's rows; `describeShareGroupAllOffsetsForGroup` describes
    // the group's initialized topics and then drops the ones the caller
    // cannot describe with no row at all, the same way Kafka hides topic
    // existence from such a caller.
    let (requested, unauthorized) = if let Some(topics) = group.topics {
        let allowed = allowed_topics(broker, image, ctx, &topics);
        let (requested, denied): (Vec<_>, Vec<_>) = topics
            .into_iter()
            .partition(|topic| allowed.contains(topic.topic_name.as_str()));
        (
            requested,
            denied.into_iter().map(unauthorized_topic).collect(),
        )
    } else {
        let metadata = ng.and_then(|ng| ng.share_state_partition_metadata(&gid));
        let initialized = initialized_topics(metadata.as_ref(), image);
        let allowed = allowed_topics(broker, image, ctx, &initialized);
        let requested = initialized
            .into_iter()
            .filter(|topic| allowed.contains(topic.topic_name.as_str()))
            .collect();
        (requested, Vec::new())
    };

    let end_offsets = NetworkLatestOffsets { broker };
    match describe_topics(persister.as_ref(), &end_offsets, image, &gid, requested).await {
        Ok(mut topics) => {
            topics.extend(unauthorized);
            DescribeShareGroupOffsetsResponseGroup {
                group_id: gid,
                topics,
                error_code: codes::NONE,
                ..Default::default()
            }
        }
        // `KafkaApis` answers a failed coordinator call with the group-level
        // error and no topics, the unauthorized ones included.
        Err((error_code, message)) => DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code,
            error_message: Some(message.to_owned()),
            ..Default::default()
        },
    }
}

/// The names among `topics` the caller may `Describe`.
fn allowed_topics(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    topics: &[DescribeShareGroupOffsetsRequestTopic],
) -> std::collections::HashSet<String> {
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        ctx.principal,
        ctx.peer,
        AclOperation::Describe,
        topics.iter().map(|topic| topic.topic_name.as_str()),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Allow)
    .map(|(name, _)| name.to_string())
    .collect()
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_log::Offset;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    };

    use super::*;
    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        coordinator::unified::share::persistence::{
            ShareGroupMetadataValue, ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
        },
        handlers::describe_share_group_offsets::test_support::{
            image_with_topic, register_topic, start_broker,
        },
    };

    /// Denies topic `Describe` on one named topic, and allows every other
    /// request (including `Describe` on the group itself). Mirrors
    /// `share_acknowledge::tests::DenyTopicRead`.
    #[derive(Debug)]
    struct DenyDescribeOnTopic(&'static str);

    impl Authorizer for DenyDescribeOnTopic {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            request: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if request.resource_type == ResourceType::Topic
                && request.operation == AclOperation::Describe
                && request.resource_name == self.0
            {
                AuthorizationResult::Deny
            } else {
                AuthorizationResult::Allow
            }
        }
    }

    fn topic(name: &str, partitions: Vec<i32>) -> DescribeShareGroupOffsetsRequestTopic {
        DescribeShareGroupOffsetsRequestTopic {
            topic_name: name.into(),
            partitions,
            ..Default::default()
        }
    }

    fn request_group(
        gid: &str,
        topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
    ) -> DescribeShareGroupOffsetsRequestGroup {
        DescribeShareGroupOffsetsRequestGroup {
            group_id: gid.into(),
            topics,
            ..Default::default()
        }
    }

    /// A row read from initialized share state over an empty log: Kafka's
    /// initial leader epoch 0, and the lag `0 - start offset - 0`, since
    /// initialize sets the delivery complete count to 0 for a start offset it
    /// is given.
    fn normal_partition(
        index: i32,
        start_offset: i64,
    ) -> DescribeShareGroupOffsetsResponsePartition {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index: index,
            start_offset,
            leader_epoch: 0,
            lag: -start_offset,
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }
    }

    fn denied_partition(index: i32) -> DescribeShareGroupOffsetsResponsePartition {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index: index,
            start_offset: -1,
            leader_epoch: 0,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".to_string()),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }
    }

    /// A denied topic requested with an empty `partitions` list answers a
    /// row with no partitions: `KafkaApis.describeShareGroupOffsetsForGroup`
    /// builds one `TOPIC_AUTHORIZATION_FAILED` row per partition the request
    /// named, and does not look the group's initialized partitions up.
    #[tokio::test]
    async fn a_denied_topic_named_with_no_partitions_has_no_partition_rows() {
        let orders_id = uuid::Uuid::from_u128(1);
        let secret_id = uuid::Uuid::from_u128(2);
        let mut image = image_with_topic("orders", orders_id);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "secret".into(),
            topic_id: secret_id,
            partitions: 2,
            replication_factor: 1,
        }));

        let (broker_handle, _dir) =
            start_broker(Arc::new(DenyDescribeOnTopic("secret")), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        register_topic(&broker, "orders", orders_id).await;
        persister
            .initialize("g3", orders_id, 0, 1, Offset(5))
            .await
            .expect("seed orders state");

        broker
            .group_coordinator
            .replay_share_group_metadata("g3", ShareGroupMetadataValue { epoch: 1 });
        broker
            .group_coordinator
            .replay_share_state_partition_metadata(
                "g3",
                ShareGroupStatePartitionMetadataValue {
                    initializing: Vec::new(),
                    initialized: vec![
                        TopicPartitionsInfo {
                            topic_id: orders_id,
                            topic_name: "orders".into(),
                            partitions: vec![0],
                        },
                        TopicPartitionsInfo {
                            topic_id: secret_id,
                            topic_name: "secret".into(),
                            partitions: vec![0, 1],
                        },
                    ],
                    deleting: Vec::new(),
                },
            );

        let principal = crate::test_support::principal("alice");
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

        let result = describe_group(
            &broker,
            Some(broker.group_coordinator.as_ref()),
            &image,
            &ctx,
            request_group(
                "g3",
                Some(vec![topic("orders", vec![0]), topic("secret", vec![])]),
            ),
        )
        .await;

        let secret_row = result
            .topics
            .iter()
            .find(|t| t.topic_name == "secret")
            .expect("denied topic row present");
        assert!(secret_row.partitions.is_empty(), "{secret_row:?}");

        broker_handle.shutdown().await;
    }

    /// Table-driven: an explicit topic list, either all allowed or one topic
    /// denied. The denied topic's row must follow the allowed one, with the
    /// all-zero topic id and `TOPIC_AUTHORIZATION_FAILED` on every requested
    /// partition.
    #[tokio::test]
    async fn explicit_topic_list_partitions_by_describe_acl() {
        struct Case {
            name: &'static str,
            authorizer: Arc<dyn Authorizer>,
            include_secret: bool,
            expected: Vec<DescribeShareGroupOffsetsResponseTopic>,
        }

        let topic_id = uuid::Uuid::from_u128(0xD5C0);
        let orders_wire_id = WireUuid(*topic_id.as_bytes());
        let orders_row = DescribeShareGroupOffsetsResponseTopic {
            topic_name: "orders".into(),
            topic_id: orders_wire_id,
            partitions: vec![normal_partition(0, 33)],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        let secret_denied_row = DescribeShareGroupOffsetsResponseTopic {
            topic_name: "secret".into(),
            topic_id: WireUuid::default(),
            partitions: vec![denied_partition(0), denied_partition(1)],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        let cases = vec![
            Case {
                name: "all topics allowed returns the normal row",
                authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
                include_secret: false,
                expected: vec![orders_row.clone()],
            },
            Case {
                name: "denied topic is appended after the allowed one",
                authorizer: Arc::new(DenyDescribeOnTopic("secret")),
                include_secret: true,
                expected: vec![orders_row.clone(), secret_denied_row.clone()],
            },
        ];

        for case in cases {
            let (broker_handle, _dir) = start_broker(case.authorizer, true).await;
            let broker = broker_handle.broker_arc_for_test();
            let persister = broker
                .group_coordinator
                .share_persister()
                .cloned()
                .expect("share persister");
            let image = image_with_topic("orders", topic_id);
            register_topic(&broker, "orders", topic_id).await;
            persister
                .initialize("g1", topic_id, 0, 1, Offset(33))
                .await
                .expect("seed state");

            let mut requested = vec![topic("orders", vec![0])];
            if case.include_secret {
                requested.push(topic("secret", vec![0, 1]));
            }

            let principal = crate::test_support::principal("alice");
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

            let result = describe_group(
                &broker,
                Some(broker.group_coordinator.as_ref()),
                &image,
                &ctx,
                request_group("g1", Some(requested)),
            )
            .await;

            let expected = DescribeShareGroupOffsetsResponseGroup {
                group_id: "g1".into(),
                topics: case.expected,
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            };
            assert!(result == expected, "case: {}", case.name);
            broker_handle.shutdown().await;
        }
    }

    /// Table-driven: a null (fetch-all) topic list. A topic the caller cannot
    /// `Describe` is silently dropped, with no row and no error code, and the
    /// rest of the group's initialized topics still come back.
    #[tokio::test]
    async fn null_topic_list_drops_undescribable_topics_silently() {
        struct Case {
            name: &'static str,
            authorizer: Arc<dyn Authorizer>,
            expected_names: Vec<&'static str>,
        }
        let cases = vec![
            Case {
                name: "all topics allowed returns every initialized topic",
                authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
                expected_names: vec!["orders", "secret"],
            },
            Case {
                name: "denied topic is silently absent",
                authorizer: Arc::new(DenyDescribeOnTopic("secret")),
                expected_names: vec!["orders"],
            },
        ];

        for case in cases {
            let (broker_handle, _dir) = start_broker(case.authorizer, true).await;
            let broker = broker_handle.broker_arc_for_test();
            let persister = broker
                .group_coordinator
                .share_persister()
                .cloned()
                .expect("share persister");

            let orders_id = uuid::Uuid::from_u128(1);
            let secret_id = uuid::Uuid::from_u128(2);
            let mut image = image_with_topic("orders", orders_id);
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: "secret".into(),
                topic_id: secret_id,
                partitions: 1,
                replication_factor: 1,
            }));
            register_topic(&broker, "orders", orders_id).await;
            persister
                .initialize("g2", orders_id, 0, 1, Offset(7))
                .await
                .expect("seed orders state");
            register_topic(&broker, "secret", secret_id).await;
            persister
                .initialize("g2", secret_id, 0, 1, Offset(9))
                .await
                .expect("seed secret state");

            broker
                .group_coordinator
                .replay_share_group_metadata("g2", ShareGroupMetadataValue { epoch: 1 });
            broker
                .group_coordinator
                .replay_share_state_partition_metadata(
                    "g2",
                    ShareGroupStatePartitionMetadataValue {
                        initializing: Vec::new(),
                        initialized: vec![
                            TopicPartitionsInfo {
                                topic_id: orders_id,
                                topic_name: "orders".into(),
                                partitions: vec![0],
                            },
                            TopicPartitionsInfo {
                                topic_id: secret_id,
                                topic_name: "secret".into(),
                                partitions: vec![0],
                            },
                        ],
                        deleting: Vec::new(),
                    },
                );

            let principal = crate::test_support::principal("alice");
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

            let result = describe_group(
                &broker,
                Some(broker.group_coordinator.as_ref()),
                &image,
                &ctx,
                request_group("g2", None),
            )
            .await;

            assert!(result.error_code == codes::NONE, "case: {}", case.name);
            let names: Vec<&str> = result
                .topics
                .iter()
                .map(|t| t.topic_name.as_str())
                .collect();
            assert!(names == case.expected_names, "case: {}", case.name);
            broker_handle.shutdown().await;
        }
    }

    /// KIP-932: `Some(vec![])` answers with the group id and an empty topic
    /// list without ever dispatching to the coordinator. Passing `ng: None`
    /// makes that concrete: if the fix regresses and the handler tries to
    /// read the persister anyway, it hits the "no persister installed"
    /// branch and answers `COORDINATOR_NOT_AVAILABLE` instead of `NONE`,
    /// failing this assertion.
    #[tokio::test]
    async fn empty_topic_list_never_reaches_the_coordinator() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let image = image_with_topic("orders", uuid::Uuid::from_u128(1));

        let principal = crate::test_support::principal("alice");
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

        let result = describe_group(
            &broker,
            None, // no coordinator: a real dispatch would fail loudly.
            &image,
            &ctx,
            request_group("g3", Some(Vec::new())),
        )
        .await;

        let expected = DescribeShareGroupOffsetsResponseGroup {
            group_id: "g3".into(),
            topics: Vec::new(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(result == expected);
        broker_handle.shutdown().await;
    }
}
