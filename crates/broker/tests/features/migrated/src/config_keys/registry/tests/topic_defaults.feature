Feature: Kafka topic configuration defaults
  A topic without overrides reports Kafka's exact defaults, including empty
  throttle lists, inherited local-retention sentinels, and unlimited long values.

  Scenario Outline: Topic default values agree with Apache Kafka
    Given the registered topic configuration key is <key>
    When DescribeConfigs reads the registered key's metadata
    Then the registered key's quoted Kafka default is <value>

    Examples:
      | key | value |
      | retention.ms | "604800000" |
      | retention.bytes | "-1" |
      | segment.bytes | "1073741824" |
      | cleanup.policy | "delete" |
      | compression.type | "producer" |
      | min.insync.replicas | "1" |
      | unclean.leader.election.enable | "false" |
      | remote.storage.enable | "false" |
      | local.retention.ms | "-2" |
      | local.retention.bytes | "-2" |
      | delete.retention.ms | "86400000" |
      | leader.replication.throttled.replicas | "" |
      | follower.replication.throttled.replicas | "" |
      | segment.ms | "604800000" |
      | segment.index.bytes | "10485760" |
      | segment.jitter.ms | "0" |
      | min.compaction.lag.ms | "0" |
      | max.compaction.lag.ms | "9223372036854775807" |
      | min.cleanable.dirty.ratio | "0.5" |
      | file.delete.delay.ms | "60000" |
      | flush.messages | "9223372036854775807" |
      | flush.ms | "9223372036854775807" |
      | index.interval.bytes | "4096" |
      | preallocate | "false" |
      | message.timestamp.type | "CreateTime" |
      | message.timestamp.after.max.ms | "3600000" |
      | message.timestamp.before.max.ms | "9223372036854775807" |
