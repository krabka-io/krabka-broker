//! Literal member subscriptions, metadata and complete partition ownership used by assignor specifications.
use {
    super::super::assignor::{
        Assignment, MemberSubscription, SubscriptionType, TopicIdPartition, TopicMetadata,
    },
    krabka_protocol::primitives::uuid::Uuid,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::DataTableRow,
    std::collections::HashMap,
};
#[derive(DataTableRow)]
pub struct TopicPartitionsInput {
    pub topic: u8,
    pub partitions: i32,
    pub replica_racks: String,
}
#[derive(DataTableRow)]
pub struct MemberAssignmentInput {
    pub member: String,
    pub rack: String,
    pub subscriptions: String,
    pub previous: String,
}
#[derive(DataTableRow)]
pub struct ExpectedMemberPartitions {
    pub member: String,
    pub partitions: String,
}
fn topic_partition_map(text: &str) -> HashMap<Uuid, Vec<i32>> {
    serde_json::from_str::<HashMap<String, Vec<i32>>>(text)
        .expect("topic-to-partitions object")
        .into_iter()
        .map(|(topic, partitions)| {
            (
                Uuid([topic.parse::<u8>().expect("topic identifier rank"); 16]),
                partitions,
            )
        })
        .collect()
}
pub fn subscription_type(builder: &str) -> SubscriptionType {
    match builder {
        "homogeneous" => SubscriptionType::Homogeneous,
        "heterogeneous" => SubscriptionType::Heterogeneous,
        _ => panic!("unknown subscription type"),
    }
}
pub fn topic_metadata(rows: Rows<TopicPartitionsInput>) -> TopicMetadata {
    let mut topics = TopicMetadata::default();
    for row in rows {
        let topic = Uuid([row.topic; 16]);
        topics.partitions_per_topic.insert(topic, row.partitions);
        for (partition, racks) in
            serde_json::from_str::<HashMap<String, Vec<String>>>(&row.replica_racks)
                .expect("partition-to-racks object")
        {
            topics.partition_racks.insert(
                TopicIdPartition {
                    topic_id: topic,
                    partition: partition.parse().expect("partition number"),
                },
                racks,
            );
        }
    }
    topics
}
pub fn member_subscriptions(rows: Rows<MemberAssignmentInput>) -> Vec<MemberSubscription> {
    rows.into_iter()
        .map(|row| MemberSubscription {
            member_id: row.member,
            rack_id: if row.rack == "absent" {
                None
            } else {
                Some(row.rack)
            },
            subscribed_topic_ids: serde_json::from_str::<Vec<u8>>(&row.subscriptions)
                .expect("topic identifier ranks")
                .into_iter()
                .map(|rank| Uuid([rank; 16]))
                .collect(),
            assigned_partitions: topic_partition_map(&row.previous),
        })
        .collect()
}
pub fn expected_assignment(rows: Rows<ExpectedMemberPartitions>) -> Assignment {
    rows.into_iter()
        .map(|row| (row.member, topic_partition_map(&row.partitions)))
        .collect()
}
