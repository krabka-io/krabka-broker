//! Full wire responses for share offset feature gates, group ACLs and missing topics.
use {
    super::*,
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer},
        handlers::test_support::StartedBroker,
        test_support::{DenyAll, ShareApiSupport, start_share_broker},
    },
    krabka_protocol::owned::{
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_response::{
            self, DescribeShareGroupOffsetsResponseGroup,
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
    },
    std::sync::Arc,
};
#[derive(Default)]
struct ShareOffsetsDescription {
    started: Option<StartedBroker>,
    response: Option<DescribeShareGroupOffsetsResponse>,
}
#[rstest::fixture]
fn share_offsets_description() -> ShareOffsetsDescription {
    let started = None;
    ShareOffsetsDescription {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a share offsets broker with API enabled {enabled} and group access allowed {allowed}"
)]
async fn describe_share_offsets_policy(
    share_offsets_description: &mut ShareOffsetsDescription,
    enabled: bool,
    allowed: bool,
) {
    let authorizer: Arc<dyn Authorizer> = if allowed {
        Arc::new(AllowAllAuthorizer)
    } else {
        Arc::new(crate::test_support::ControllerPeerAllowed(DenyAll))
    };
    let started = start_share_broker()
        .authorizer(authorizer)
        .support(if enabled {
            ShareApiSupport::Enabled
        } else {
            ShareApiSupport::Disabled
        })
        .call()
        .await;
    share_offsets_description.started = Some(StartedBroker {
        broker: started.broker,
        dir: started.dir,
    });
}
#[rstest_bdd_macros::when("DescribeShareGroupOffsets requests these groups and partitions")]
async fn describe_share_offsets_request(
    share_offsets_description: &mut ShareOffsetsDescription,
    datatable: Vec<Vec<String>>,
) {
    let request = DescribeShareGroupOffsetsRequest {
        groups: datatable
            .into_iter()
            .skip(1)
            .map(|cells| DescribeShareGroupOffsetsRequestGroup {
                group_id: cells[0].clone(),
                topics: Some(vec![DescribeShareGroupOffsetsRequestTopic {
                    topic_name: cells[1].clone(),
                    partitions: cells[2]
                        .split(',')
                        .map(|p| p.parse().expect("partition index"))
                        .collect(),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let broker = share_offsets_description
        .started
        .as_ref()
        .expect("started broker")
        .broker
        .broker_arc_for_test();
    krabka_macros::request_test_identity! { bind(principal, peer, ctx); user ["alice"]; client ["admin-client"]; };
    share_offsets_description.response = Some(
        handle(
            &broker,
            request,
            describe_share_group_offsets_response::MAX_VERSION,
            &ctx,
        )
        .await
        .expect("handle"),
    );
}
#[rstest_bdd_macros::then("the complete share offset response contains these group rows")]
async fn describe_share_offsets_results(
    share_offsets_description: &mut ShareOffsetsDescription,
    datatable: Vec<Vec<String>>,
) {
    let groups = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            let code = cells[1].parse().expect("group error code");
            let topics = if cells[2] == "none" {
                Vec::new()
            } else {
                vec![DescribeShareGroupOffsetsResponseTopic {
                    topic_name: cells[2].clone(),
                    partitions: cells[3]
                        .split(',')
                        .map(|partition| DescribeShareGroupOffsetsResponsePartition {
                            partition_index: partition.parse().expect("partition index"),
                            start_offset: -1,
                            leader_epoch: 0,
                            lag: -1,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }]
            };
            DescribeShareGroupOffsetsResponseGroup {
                group_id: cells[0].clone(),
                error_code: code,
                topics,
                ..Default::default()
            }
        })
        .collect();
    let expected = DescribeShareGroupOffsetsResponse {
        groups,
        ..Default::default()
    };
    assert2::assert!(
        share_offsets_description
            .response
            .as_ref()
            .expect("description response")
            == &expected
    );
    share_offsets_description
        .started
        .take()
        .expect("started broker")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_describe_share_offsets.feature",
    name = "Disabled API retains every requested group"
)]
#[tokio::test]
async fn describe_share_offset_case_0(share_offsets_description: ShareOffsetsDescription) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_describe_share_offsets.feature",
    name = "Group ACL denial preserves its identity"
)]
#[tokio::test]
async fn describe_share_offset_case_1(share_offsets_description: ShareOffsetsDescription) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_describe_share_offsets.feature",
    name = "Missing topics report uninitialized data without errors"
)]
#[tokio::test]
async fn describe_share_offset_case_2(share_offsets_description: ShareOffsetsDescription) {}
