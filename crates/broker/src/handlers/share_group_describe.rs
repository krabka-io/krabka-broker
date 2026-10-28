//! `ShareGroupDescribe` (`api_key` 77), from KIP-932. It returns one
//! `DescribedGroup` per requested `group_id`, built from the share actor's
//! `Describe` view, as Kafka's `KafkaApis.handleShareGroupDescribe` does.
//!
//! `network::dispatch` intercepts this request inline, not through
//! `build_table`, so the handler receives the per-connection principal and the
//! peer `SocketAddr` for the per-group `Describe` ACL gate.

#[cfg(test)]
use crate::authorizer::SimpleAclAuthorizer;
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{GroupType, share::actor::ShareGroupActorMessage},
        error::BrokerError,
        features::share_groups_enabled,
        handlers::{
            ApiVersion, RequestContext,
            authorized_operations::{DescribedGroupRow as _, fill_group_authorized_operations},
            consumer_group_describe::{DescribedTopics, hide_undescribable_topics},
            group_coordinator_error, group_describe_denied, share_group_not_found_message,
        },
        task_util::{AskError, ask},
    },
    krabka_protocol::owned::{
        share_group_describe_request::ShareGroupDescribeRequest,
        share_group_describe_response::{DescribedGroup, ShareGroupDescribeResponse},
    },
};

impl DescribedTopics for DescribedGroup {
    fn topics(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .flat_map(|member| &member.assignment.topic_partitions)
            .map(|topic| topic.topic_name.as_str())
    }
}

pub async fn handle(
    broker: &Broker,
    req: ShareGroupDescribeRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<ShareGroupDescribeResponse, BrokerError> {
    // Kafka's `isShareGroupProtocolEnabled` gate comes before any ACL check,
    // and `getErrorResponse` answers every requested group. Share groups are
    // on from a finalized `share.version` of 1.
    let image = broker.controller.current_image();
    if !share_groups_enabled(&image) {
        let groups = req
            .group_ids
            .iter()
            .map(|gid| DescribedGroup::error_row(gid, codes::UNSUPPORTED_VERSION, None))
            .collect();
        return Ok(response(groups));
    }

    let authorizer = broker.config.authorizer.as_ref();
    let coordinator = &broker.group_coordinator;
    // Kafka adds the GROUP_AUTHORIZATION_FAILED rows first, then the
    // coordinator results.
    let mut groups: Vec<DescribedGroup> = Vec::new();
    let mut invalid: Vec<DescribedGroup> = Vec::new();
    let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        if group_describe_denied(authorizer, &image, ctx, gid) {
            groups.push(DescribedGroup::error_row(
                gid,
                codes::GROUP_AUTHORIZATION_FAILED,
                None,
            ));
            continue;
        }
        // GroupCoordinatorService.shareGroupDescribe rejects an empty id
        // before it routes the group to a shard, and its row comes ahead of
        // the shard results.
        if gid.is_empty() {
            invalid.push(DescribedGroup::error_row("", codes::INVALID_GROUP_ID, None));
            continue;
        }
        if let Some(error_code) = group_coordinator_error(broker, gid) {
            described.push(DescribedGroup::error_row(gid, error_code, None));
            continue;
        }
        let handle = match coordinator.group_type(gid) {
            Some(GroupType::Share) | None => coordinator.find_share(gid),
            Some(_) => None,
        };
        let Some(handle) = handle else {
            described.push(DescribedGroup::error_row(
                gid,
                codes::GROUP_ID_NOT_FOUND,
                Some(share_group_not_found_message(coordinator, gid)),
            ));
            continue;
        };

        let asked = ask(&handle.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await;
        described.push(match asked {
            Ok(view) => view.into_described_group(&image),
            Err(AskError::Closed) => {
                DescribedGroup::error_row(gid, codes::COORDINATOR_LOAD_IN_PROGRESS, None)
            }
            Err(AskError::Dropped) => {
                DescribedGroup::error_row(gid, codes::UNKNOWN_SERVER_ERROR, None)
            }
        });
    }

    // KIP-430: the group operations bitfield, only on opt-in and only for rows
    // that came back clean.
    fill_group_authorized_operations(
        authorizer,
        &image,
        ctx,
        req.include_authorized_operations,
        &mut described,
    );
    groups.extend(invalid);
    groups.extend(described);

    // Clients may not see topics they cannot `Describe`: a group whose
    // assignment names one is replaced by Kafka's TOPIC_AUTHORIZATION_FAILED
    // row with no members.
    hide_undescribable_topics(authorizer, &image, ctx, &mut groups);

    Ok(response(groups))
}

fn response(groups: Vec<DescribedGroup>) -> ShareGroupDescribeResponse {
    ShareGroupDescribeResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::Broker,
            handlers::{
                group_heartbeat_test_support::{acl as group_acl, topic_with_partitions},
                test_support as handler_fixture_support,
            },
            test_support::{
                DenyAll, ShareApiSupport, StartedBroker as HandlerStartedBroker, start_share_broker,
            },
        },
        assert2::assert,
        krabka_metadata::{AclOperation, ResourceType},
        krabka_protocol::{owned::share_group_describe_response, primitives::uuid::Uuid},
        std::{collections::HashSet, sync::Arc},
    };

    fn request(group_ids: &[&str]) -> ShareGroupDescribeRequest {
        ShareGroupDescribeRequest {
            group_ids: group_ids.iter().map(|g| (*g).to_owned()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        }
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    #[tokio::test]
    async fn handle_denied_groups_preserve_group_ids_and_error_codes() {
        let version = share_group_describe_response::MAX_VERSION;
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_share_broker()
            .authorizer(Arc::new(DenyAll))
            .call()
            .await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };
        let resp = handle(&broker, request(&["g1", "g2"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: ["g1", "g2"]
                .map(|group_id| DescribedGroup {
                    group_id: group_id.into(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka's protocol gate answers every group with `UNSUPPORTED_VERSION`,
    /// before any ACL check.
    #[tokio::test]
    async fn handle_disabled_feature_wins_even_when_share_actor_exists() {
        let version = share_group_describe_response::MAX_VERSION;
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_share_broker()
            .authorizer(Arc::new(DenyAll))
            .support(ShareApiSupport::Disabled)
            .call()
            .await;
        let broker = broker_handle.broker_arc_for_test();
        broker.group_coordinator.mark_share("g1");
        let _actor = broker.group_coordinator.get_or_create_share("g1");
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };
        let resp = handle(&broker, request(&["g1"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![DescribedGroup {
                error_code: codes::UNSUPPORTED_VERSION,
                group_id: "g1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    fn acl(resource_type: ResourceType, name: &str) -> krabka_metadata::MetadataRecord {
        group_acl(resource_type, name, AclOperation::Describe)
    }

    fn topic(
        name: &str,
        topic_id: uuid::Uuid,
        node: krabka_raft::NodeId,
    ) -> Vec<krabka_metadata::MetadataRecord> {
        topic_with_partitions()
            .name(name)
            .topic_id(topic_id)
            .partitions(1)
            .node(node)
            .call()
    }

    /// Seeds share group `group_id` with member `m1` assigned partition 0 of
    /// each of `topics`, plus partition 0 of a topic the image does not hold.
    async fn seed_group(broker: &Broker, group_id: &str, topics: &[uuid::Uuid]) {
        use crate::coordinator::unified::{
            ShareGroupSeed,
            share::persistence::{
                ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            },
        };
        let mut assigned: Vec<_> = topics
            .iter()
            .map(|t| (Uuid(*t.as_bytes()), vec![0]))
            .collect();
        assigned.push((Uuid([0xEE; 16]), vec![0]));
        let coordinator = &broker.group_coordinator;
        coordinator.mark_share(group_id);
        let handle = coordinator.get_or_create_share(group_id);
        handle
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                group_epoch: 2,
                target_epoch: 2,
                assignment_timestamp_ms: 0,
                members: [("m1".to_owned(), ShareGroupMemberMetadataValue {
                    rack_id: None,
                    client_id: "client".into(),
                    client_host: "/127.0.0.1".into(),
                    subscribed_topic_names: vec!["t".into()],
                })]
                .into(),
                current_per_member: [("m1".to_owned(), ShareGroupCurrentMemberAssignmentValue {
                    member_epoch: 2,
                    previous_member_epoch: 1,
                    assigned_partitions: assigned,
                })]
                .into(),
                ..ShareGroupSeed::default()
            }))
            .await
            .expect("seed share group");
    }

    #[derive(Default)]
    struct ShareDescriptionScenario {
        started: Option<HandlerStartedBroker>,
        response: Option<ShareGroupDescribeResponse>,
    }
    #[rstest::fixture]
    fn share_description() -> ShareDescriptionScenario {
        let started = None;
        ShareDescriptionScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "alice may Describe groups g, h and missing, and topic t with UUID 1, but not secret with UUID 2"
    )]
    async fn share_description_acls(share_description: &mut ShareDescriptionScenario) {
        let started = start_share_broker()
            .authorizer(Arc::new(SimpleAclAuthorizer::new(HashSet::new())))
            .call()
            .await;
        let broker = started.broker.broker_arc_for_test();
        let node = krabka_raft::NodeId(started.broker.node_id());
        let mut records = vec![
            acl(ResourceType::Group, "g"),
            acl(ResourceType::Group, "h"),
            acl(ResourceType::Group, "missing"),
            acl(ResourceType::Topic, "t"),
        ];
        records.extend(topic("t", uuid::Uuid::from_u128(1), node));
        records.extend(topic("secret", uuid::Uuid::from_u128(2), node));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("ACLs and topics");
        share_description.started = Some(started);
    }
    #[rstest_bdd_macros::given(
        "stable share groups at epoch 2 have member m1 at epoch 2, client client at /127.0.0.1 subscribed to t, with unknown UUID eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and these partition-zero assignments"
    )]
    async fn share_description_members(
        share_description: &mut ShareDescriptionScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let broker = share_description
            .started
            .as_ref()
            .expect("started broker")
            .broker
            .broker_arc_for_test();
        for cells in datatable.into_iter().skip(1) {
            let topics = cells[1]
                .split(',')
                .map(|id| uuid::Uuid::from_u128(id.parse().expect("topic UUID")))
                .collect::<Vec<_>>();
            seed_group(&broker, &cells[0], &topics).await;
        }
    }
    #[rstest_bdd_macros::when(
        "alice describes share groups {groups} including authorized operations"
    )]
    async fn describe_share_groups(
        share_description: &mut ShareDescriptionScenario,
        groups: String,
    ) {
        let broker = share_description
            .started
            .as_ref()
            .expect("started broker")
            .broker
            .broker_arc_for_test();
        krabka_macros::request_test_identity! { bind(principal, peer, ctx); user ["alice"]; client ["admin-client"]; };
        let req = ShareGroupDescribeRequest {
            group_ids: groups.split(',').map(str::to_owned).collect(),
            include_authorized_operations: true,
            ..Default::default()
        };
        share_description.response = Some(
            handle(
                &broker,
                req,
                share_group_describe_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle"),
        );
    }
    #[rstest_bdd_macros::then("the complete ordered share group descriptions are")]
    async fn share_group_descriptions(
        share_description: &mut ShareDescriptionScenario,
        datatable: Vec<Vec<String>>,
    ) {
        use krabka_protocol::owned::{
            common::share_group_describe_response::{
                assignment::Assignment, topic_partitions::TopicPartitions,
            },
            share_group_describe_response::Member,
        };
        let groups = datatable
            .into_iter()
            .skip(1)
            .map(|cells| {
                let code: i16 = cells[1].parse().expect("group error code");
                if code != 0 {
                    return DescribedGroup::error_row(
                        &cells[0],
                        code,
                        (cells[2] != "null").then(|| cells[2].clone()),
                    );
                }
                DescribedGroup {
                    group_id: cells[0].clone(),
                    group_state: cells[3].clone(),
                    group_epoch: cells[4].parse().expect("group epoch"),
                    assignment_epoch: cells[5].parse().expect("assignment epoch"),
                    assignor_name: cells[6].clone(),
                    authorized_operations: cells[7].parse().expect("authorized operations"),
                    members: vec![Member {
                        member_id: "m1".into(),
                        member_epoch: 2,
                        client_id: "client".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                        assignment: Assignment {
                            topic_partitions: vec![TopicPartitions {
                                topic_id: Uuid(*uuid::Uuid::from_u128(1).as_bytes()),
                                topic_name: cells[8].clone(),
                                partitions: vec![0],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            })
            .collect();
        let expected = ShareGroupDescribeResponse {
            groups,
            ..Default::default()
        };
        assert!(
            share_description
                .response
                .as_ref()
                .expect("description response")
                == &expected
        );
        share_description
            .started
            .take()
            .expect("started broker")
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_group_describe.feature",
        name = "Group ACLs, topic ACLs and missing groups preserve Kafka response rows"
    )]
    #[tokio::test]
    async fn handle_matches_kafka_with_acls(share_description: ShareDescriptionScenario) {}

    /// `INVALID_GROUP_ID` for an empty id, ahead of the shard rows, and
    /// Kafka's message for a group of another type.
    #[tokio::test]
    async fn handle_refuses_empty_and_foreign_group_ids() {
        let version = share_group_describe_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_share_allow_all()];
        };
        let _classic = broker.group_coordinator.get_or_create_classic("classic");
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };

        let resp = handle(&broker, request(&["classic", ""]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![
                DescribedGroup::error_row("", codes::INVALID_GROUP_ID, None),
                DescribedGroup::error_row(
                    "classic",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group classic is not a share group.".into()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
