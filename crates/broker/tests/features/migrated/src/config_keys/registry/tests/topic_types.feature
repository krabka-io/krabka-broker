Feature: Kafka topic configuration types
  Each registered topic key retains the ConfigDef type read by a Kafka
  AdminClient from DescribeConfigs, rather than guessing a type from its value.

  Scenario Outline: Registered topic key types agree with Apache Kafka
    Given the registered topic configuration key is <key>
    When DescribeConfigs reads the registered key's metadata
    Then the registered key's Kafka type is <kind>

    Examples:
      | key | kind |
      | retention.ms | long |
      | retention.bytes | long |
      | segment.bytes | int |
      | cleanup.policy | list |
      | compression.type | string |
      | min.insync.replicas | int |
      | unclean.leader.election.enable | boolean |
      | remote.storage.enable | boolean |
      | local.retention.ms | long |
      | local.retention.bytes | long |
      | delete.retention.ms | long |
      | leader.replication.throttled.replicas | list |
      | follower.replication.throttled.replicas | list |
      | segment.ms | long |
      | segment.index.bytes | int |
      | segment.jitter.ms | long |
      | min.compaction.lag.ms | long |
      | max.compaction.lag.ms | long |
      | min.cleanable.dirty.ratio | double |
      | file.delete.delay.ms | long |
      | flush.messages | long |
      | flush.ms | long |
      | index.interval.bytes | int |
      | preallocate | boolean |
      | message.timestamp.type | string |
      | message.timestamp.after.max.ms | long |
      | message.timestamp.before.max.ms | long |
