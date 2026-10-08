//! Consumer heartbeat and native routing fixtures.

use krabka_protocol::owned::{
    consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
    consumer_group_heartbeat_response::Assignment,
};

pub fn consumer_heartbeat(
    group: impl Into<String>,
    member: impl Into<String>,
    epoch: i32,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member.into(),
        member_epoch: epoch,
        ..Default::default()
    }
}

pub fn joining_consumer(
    group: impl Into<String>,
    member: impl Into<String>,
    timeout: i32,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        rebalance_timeout_ms: timeout,
        topic_partitions: Some(vec![]),
        ..consumer_heartbeat(group, member, 0)
    }
}

pub fn reported_assignment(assignment: &Assignment) -> Vec<TopicPartitions> {
    assignment
        .topic_partitions
        .iter()
        .map(|topic| TopicPartitions {
            topic_id: topic.topic_id,
            partitions: topic.partitions.clone(),
            ..Default::default()
        })
        .collect()
}

/// A native consumer with the routing and proactive-validation fixtures' timing.
///
/// # Panics
/// Panics if the consumer cannot connect or subscribe.
pub async fn routing_consumer(
    bootstrap: &str,
    client_id: &str,
    group: &str,
    topic: &str,
    reset: krabka_client_consumer::AutoOffsetReset,
) -> krabka_client_consumer::Consumer {
    krabka_client_consumer::Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group)
        .session_timeout(krabka_units::secs(30))
        .max_poll_interval(krabka_units::secs(2))
        .heartbeat_interval(krabka_units::secs(1))
        .auto_offset_reset(reset)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap()
}

/// Poll the routing consumer for distinct UTF-8-lossy values within its original bounds.
/// Each record also reaches the caller's partition-specific observer.
///
/// # Panics
/// Panics if polling fails.
pub async fn collect_routing_values(
    consumer: &mut krabka_client_consumer::Consumer,
    count: usize,
    mut observe: impl FnMut(i32, String),
) -> std::collections::HashSet<String> {
    let mut seen = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while seen.len() < count && std::time::Instant::now() < deadline {
        for record in consumer.poll(krabka_units::millis(300)).await.unwrap() {
            let value =
                String::from_utf8_lossy(record.value.as_deref().unwrap_or(&[])).into_owned();
            seen.insert(value.clone());
            observe(record.partition, value);
        }
    }
    seen
}
