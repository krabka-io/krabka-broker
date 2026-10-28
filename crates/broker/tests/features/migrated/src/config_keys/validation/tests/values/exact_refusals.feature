Feature: Topic refusals preserve Kafka ConfigException text
  Operators receive the exact unknown-key, parse, range, enum, and list messages.

  Scenario Outline: Topic refusals preserve Kafka ConfigException text
    Given a topic alteration supplies key <key> and quoted value <value>
    When the released per-key topic validator reads the supplied value
    Then the topic refusal carries exact Kafka message <message>

    Examples:
      | key | value | message |
      | foo | "1" | Unknown topic config name: foo |
      | max.message.bytes | "-1" | Invalid value -1 for configuration max.message.bytes: Value must be at least 0 |
      | compression.lz4.level | "18" | Invalid value 18 for configuration compression.lz4.level: Value must be no more than 17 |
      | compression.gzip.level | "0" | Invalid value 0 for configuration compression.gzip.level: Value must be between 1 and 9 or equal to -1 |
      | retention.ms | "abc" | Invalid value abc for configuration retention.ms: Not a number of type LONG |
      | segment.bytes | "2147483648" | Invalid value 2147483648 for configuration segment.bytes: Not a number of type INT |
      | preallocate | "yes" | Invalid value yes for configuration preallocate: Expected value to be either true or false |
      | compression.type | "none" | Invalid value none for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer |
      | message.timestamp.type | "x" | Invalid value x for configuration message.timestamp.type: String must be one of: CreateTime, LogAppendTime |
      | cleanup.policy | "foo" | Invalid value foo for configuration cleanup.policy: String must be one of: compact, delete |
      | cleanup.policy | "delete,delete" | Configuration 'cleanup.policy' values must not be duplicated. |
      | cleanup.policy | "delete," | Configuration 'cleanup.policy' values must not be empty. |
      | min.cleanable.dirty.ratio | "2" | Invalid value 2.0 for configuration min.cleanable.dirty.ratio: Value must be no more than 1 |
      | follower.replication.throttled.replicas | "0 : 1" | Invalid value [0 : 1] for configuration follower.replication.throttled.replicas: follower.replication.throttled.replicas must be the literal '*' or a list of replicas in the following format: [partitionId]:[brokerId],[partitionId]:[brokerId],... |
