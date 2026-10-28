//! Complete, field-by-field observations of persisted coordinator hydration state.

use {
    super::super::{
        persistence_next_gen::{
            AssignedTopicPartitions, ClassicMemberMetadata, CurrentMemberAssignmentValue,
            CurrentTopicPartitions, MemberMetadataValue, RegularExpressionValue,
            TargetAssignmentMemberValue,
        },
        seeds::{GroupSeed, ShareGroupSeed, StreamsGroupSeed},
        share::persistence::{
            DeletingTopic, ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            ShareGroupStatePartitionMetadataValue, ShareGroupTargetAssignmentMemberValue,
            TopicPartitionsInfo,
        },
        streams::{
            description::DescriptionEpochs,
            persistence::{
                StoredCopartitionGroup, StoredSubtopology, StoredTopicInfo, StreamsEndpoint,
                StreamsGroupCurrentMemberAssignmentValue, StreamsGroupMemberMetadataValue,
                StreamsGroupTargetAssignmentMemberValue, StreamsGroupTopologyValue,
            },
        },
    },
    serde_json::{Value, json},
};

pub fn consumer_seed(value: &GroupSeed) -> Value {
    let GroupSeed {
        group_epoch,
        metadata_hash,
        target_epoch,
        assignment_timestamp_ms,
        members,
        target_per_member,
        current_per_member,
        resolved_regexes,
        has_subscription_metadata_record,
    } = value;
    json!({
        "group_epoch": group_epoch,
        "metadata_hash": metadata_hash,
        "target_epoch": target_epoch,
        "assignment_timestamp_ms": assignment_timestamp_ms,
        "members": members.iter().map(|(id, value)| (id.clone(), consumer_member(value))).collect::<serde_json::Map<String, Value>>(),
        "target_per_member": target_per_member.iter().map(|(id, value)| (id.clone(), consumer_target(value))).collect::<serde_json::Map<String, Value>>(),
        "current_per_member": current_per_member.iter().map(|(id, value)| (id.clone(), consumer_current(value))).collect::<serde_json::Map<String, Value>>(),
        "resolved_regexes": resolved_regexes.iter().map(|(id, value)| (id.clone(), resolved_regex(value))).collect::<serde_json::Map<String, Value>>(),
        "has_subscription_metadata_record": has_subscription_metadata_record,
    })
}

pub fn share_seed(value: &ShareGroupSeed) -> Value {
    let ShareGroupSeed {
        group_epoch,
        metadata_hash,
        target_epoch,
        assignment_timestamp_ms,
        members,
        target_per_member,
        current_per_member,
        state_partition_metadata,
    } = value;
    json!({
        "group_epoch": group_epoch,
        "metadata_hash": metadata_hash,
        "target_epoch": target_epoch,
        "assignment_timestamp_ms": assignment_timestamp_ms,
        "members": members.iter().map(|(id, value)| (id.clone(), share_member(value))).collect::<serde_json::Map<String, Value>>(),
        "target_per_member": target_per_member.iter().map(|(id, value)| (id.clone(), share_target(value))).collect::<serde_json::Map<String, Value>>(),
        "current_per_member": current_per_member.iter().map(|(id, value)| (id.clone(), share_current(value))).collect::<serde_json::Map<String, Value>>(),
        "state_partition_metadata": share_state(state_partition_metadata),
    })
}

pub fn streams_seed(value: &StreamsGroupSeed) -> Value {
    let StreamsGroupSeed {
        group_epoch,
        metadata_hash,
        validated_topology_epoch,
        last_assignment_configs,
        description_epochs: description_epochs_value,
        assignment_epoch,
        assignment_timestamp_ms,
        topology,
        members,
        target_per_member,
        current_per_member,
    } = value;
    json!({
        "group_epoch": group_epoch,
        "metadata_hash": metadata_hash,
        "validated_topology_epoch": validated_topology_epoch,
        "last_assignment_configs": last_assignment_configs,
        "description_epochs": description_epochs(description_epochs_value),
        "assignment_epoch": assignment_epoch,
        "assignment_timestamp_ms": assignment_timestamp_ms,
        "topology": topology.as_ref().map(streams_topology),
        "members": members.iter().map(|(id, value)| (id.clone(), streams_member(value))).collect::<serde_json::Map<String, Value>>(),
        "target_per_member": target_per_member.iter().map(|(id, value)| (id.clone(), streams_target(value))).collect::<serde_json::Map<String, Value>>(),
        "current_per_member": current_per_member.iter().map(|(id, value)| (id.clone(), streams_current(value))).collect::<serde_json::Map<String, Value>>(),
    })
}

pub fn consumer_member(value: &MemberMetadataValue) -> Value {
    let MemberMetadataValue {
        instance_id,
        rack_id,
        client_id,
        client_host,
        subscribed_topic_names,
        subscribed_topic_regex,
        server_assignor,
        rebalance_timeout_ms,
        classic,
    } = value;
    json!({
        "instance_id": instance_id,
        "rack_id": rack_id,
        "client_id": client_id,
        "client_host": client_host,
        "subscribed_topic_names": subscribed_topic_names,
        "subscribed_topic_regex": subscribed_topic_regex,
        "server_assignor": server_assignor,
        "rebalance_timeout_ms": rebalance_timeout_ms,
        "classic": classic.as_ref().map(classic_member),
    })
}

pub fn classic_member(value: &ClassicMemberMetadata) -> Value {
    let ClassicMemberMetadata {
        session_timeout_ms,
        supported_protocols,
    } = value;
    json!({
        "session_timeout_ms": session_timeout_ms,
        "supported_protocols": supported_protocols.iter().map(|(name, bytes)| json!({"name":name,"metadata_hex":hex::encode(bytes)})).collect::<Vec<_>>(),
    })
}

pub fn consumer_target_topic(value: &AssignedTopicPartitions) -> Value {
    let AssignedTopicPartitions {
        topic_id,
        partitions,
    } = value;
    json!({
        "topic_id": uuid::Uuid::from_bytes(topic_id.0).to_string(),
        "partitions": partitions,
    })
}

pub fn consumer_target(value: &TargetAssignmentMemberValue) -> Value {
    let TargetAssignmentMemberValue { topic_partitions } = value;
    json!({
        "topic_partitions": topic_partitions.iter().map(consumer_target_topic).collect::<Vec<_>>(),
    })
}

pub fn consumer_current_topic(value: &CurrentTopicPartitions) -> Value {
    let CurrentTopicPartitions {
        topic_id,
        partitions,
        assignment_epochs,
    } = value;
    json!({
        "topic_id": uuid::Uuid::from_bytes(topic_id.0).to_string(),
        "partitions": partitions,
        "assignment_epochs": assignment_epochs,
    })
}

pub fn consumer_current(value: &CurrentMemberAssignmentValue) -> Value {
    let CurrentMemberAssignmentValue {
        member_epoch,
        previous_member_epoch,
        state,
        assigned_partitions,
        partitions_pending_revocation,
    } = value;
    json!({
        "member_epoch": member_epoch,
        "previous_member_epoch": previous_member_epoch,
        "state": *state as i8,
        "assigned_partitions": assigned_partitions.iter().map(consumer_current_topic).collect::<Vec<_>>(),
        "partitions_pending_revocation": partitions_pending_revocation.iter().map(consumer_current_topic).collect::<Vec<_>>(),
    })
}

pub fn resolved_regex(value: &RegularExpressionValue) -> Value {
    let RegularExpressionValue {
        topics,
        version,
        timestamp_ms,
    } = value;
    json!({
        "topics": topics,
        "version": version,
        "timestamp_ms": timestamp_ms,
    })
}

pub fn share_member(value: &ShareGroupMemberMetadataValue) -> Value {
    let ShareGroupMemberMetadataValue {
        rack_id,
        client_id,
        client_host,
        subscribed_topic_names,
    } = value;
    json!({
        "rack_id": rack_id,
        "client_id": client_id,
        "client_host": client_host,
        "subscribed_topic_names": subscribed_topic_names,
    })
}

pub fn share_target(value: &ShareGroupTargetAssignmentMemberValue) -> Value {
    let ShareGroupTargetAssignmentMemberValue { topic_partitions } = value;
    json!({
        "topic_partitions": topic_partitions.iter().map(|(topic, partitions)| json!({"topic_id":uuid::Uuid::from_bytes(topic.0).to_string(),"partitions":partitions})).collect::<Vec<_>>(),
    })
}

pub fn share_current(value: &ShareGroupCurrentMemberAssignmentValue) -> Value {
    let ShareGroupCurrentMemberAssignmentValue {
        member_epoch,
        previous_member_epoch,
        assigned_partitions,
    } = value;
    json!({
        "member_epoch": member_epoch,
        "previous_member_epoch": previous_member_epoch,
        "assigned_partitions": assigned_partitions.iter().map(|(topic, partitions)| json!({"topic_id":uuid::Uuid::from_bytes(topic.0).to_string(),"partitions":partitions})).collect::<Vec<_>>(),
    })
}

pub fn share_topic(value: &TopicPartitionsInfo) -> Value {
    let TopicPartitionsInfo {
        topic_id,
        topic_name,
        partitions,
    } = value;
    json!({
        "topic_id": topic_id.to_string(),
        "topic_name": topic_name,
        "partitions": partitions,
    })
}

pub fn share_deleting_topic(value: &DeletingTopic) -> Value {
    let DeletingTopic {
        topic_id,
        topic_name,
    } = value;
    json!({
        "topic_id": topic_id.to_string(),
        "topic_name": topic_name,
    })
}

pub fn share_state(value: &ShareGroupStatePartitionMetadataValue) -> Value {
    let ShareGroupStatePartitionMetadataValue {
        initializing,
        initialized,
        deleting,
    } = value;
    json!({
        "initializing": initializing.iter().map(share_topic).collect::<Vec<_>>(),
        "initialized": initialized.iter().map(share_topic).collect::<Vec<_>>(),
        "deleting": deleting.iter().map(share_deleting_topic).collect::<Vec<_>>(),
    })
}

pub fn streams_endpoint(value: &StreamsEndpoint) -> Value {
    let StreamsEndpoint { host, port } = value;
    json!({
        "host": host,
        "port": port,
    })
}

pub fn streams_member(value: &StreamsGroupMemberMetadataValue) -> Value {
    let StreamsGroupMemberMetadataValue {
        instance_id,
        rack_id,
        client_id,
        client_host,
        process_id,
        user_endpoint,
        client_tags,
        rebalance_timeout_ms,
        topology_epoch,
    } = value;
    json!({
        "instance_id": instance_id,
        "rack_id": rack_id,
        "client_id": client_id,
        "client_host": client_host,
        "process_id": process_id,
        "user_endpoint": user_endpoint.as_ref().map(streams_endpoint),
        "client_tags": client_tags,
        "rebalance_timeout_ms": rebalance_timeout_ms,
        "topology_epoch": topology_epoch,
    })
}

pub fn streams_target(value: &StreamsGroupTargetAssignmentMemberValue) -> Value {
    let StreamsGroupTargetAssignmentMemberValue {
        active,
        standby,
        warmup,
    } = value;
    json!({
        "active": active,
        "standby": standby,
        "warmup": warmup,
    })
}

pub fn streams_current(value: &StreamsGroupCurrentMemberAssignmentValue) -> Value {
    let StreamsGroupCurrentMemberAssignmentValue {
        member_epoch,
        previous_member_epoch,
        state,
        active,
        standby,
        warmup,
        active_pending_revocation,
        standby_pending_revocation,
        warmup_pending_revocation,
        active_epochs,
        active_pending_revocation_epochs,
    } = value;
    json!({
        "member_epoch": member_epoch,
        "previous_member_epoch": previous_member_epoch,
        "state": *state as i8,
        "active": active,
        "standby": standby,
        "warmup": warmup,
        "active_pending_revocation": active_pending_revocation,
        "standby_pending_revocation": standby_pending_revocation,
        "warmup_pending_revocation": warmup_pending_revocation,
        "active_epochs": active_epochs,
        "active_pending_revocation_epochs": active_pending_revocation_epochs,
    })
}

pub fn streams_topic(value: &StoredTopicInfo) -> Value {
    let StoredTopicInfo {
        name,
        partitions,
        replication_factor,
        topic_configs,
    } = value;
    json!({
        "name": name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "topic_configs": topic_configs,
    })
}

pub fn streams_copartition(value: &StoredCopartitionGroup) -> Value {
    let StoredCopartitionGroup {
        source_topics,
        source_topic_regex,
        repartition_source_topics,
    } = value;
    json!({
        "source_topics": source_topics,
        "source_topic_regex": source_topic_regex,
        "repartition_source_topics": repartition_source_topics,
    })
}

pub fn streams_subtopology(value: &StoredSubtopology) -> Value {
    let StoredSubtopology {
        subtopology_id,
        source_topics,
        source_topic_regex,
        repartition_sink_topics,
        state_changelog_topics,
        repartition_source_topics,
        copartition_groups,
    } = value;
    json!({
        "subtopology_id": subtopology_id,
        "source_topics": source_topics,
        "source_topic_regex": source_topic_regex,
        "repartition_sink_topics": repartition_sink_topics,
        "state_changelog_topics": state_changelog_topics.iter().map(streams_topic).collect::<Vec<_>>(),
        "repartition_source_topics": repartition_source_topics.iter().map(streams_topic).collect::<Vec<_>>(),
        "copartition_groups": copartition_groups.iter().map(streams_copartition).collect::<Vec<_>>(),
    })
}

pub fn streams_topology(value: &StreamsGroupTopologyValue) -> Value {
    let StreamsGroupTopologyValue {
        epoch,
        subtopologies,
    } = value;
    json!({
        "epoch": epoch,
        "subtopologies": subtopologies.iter().map(streams_subtopology).collect::<Vec<_>>(),
    })
}

pub fn description_epochs(value: &DescriptionEpochs) -> Value {
    let DescriptionEpochs { stored, failed } = value;
    json!({
        "stored": stored,
        "failed": failed,
    })
}
