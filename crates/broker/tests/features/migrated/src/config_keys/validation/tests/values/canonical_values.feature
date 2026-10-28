Feature: Topic canonicalization follows Kafka ConfigDef parsing
  Whitespace, booleans, doubles, lists, and replica-throttle syntax retain their canonical outcomes.

  Scenario Outline: Topic canonicalization follows Kafka ConfigDef parsing
    Given a topic alteration supplies key <key> and quoted value <value>
    When the supplied topic value is canonicalized in released mode
    Then the canonical topic outcome is <expected>

    Examples:
      | key | value | expected |
      | preallocate | "TRUE" | "true" |
      | remote.log.copy.disable | " false " | "false" |
      | preallocate | "yes" | rejected |
      | retention.ms | " 1000 " | "1000" |
      | segment.bytes | "1048576 " | "1048576" |
      | compression.type | " gzip" | "gzip" |
      | min.cleanable.dirty.ratio | "0.5d" | "0.5" |
      | min.cleanable.dirty.ratio | "1" | "1.0" |
      | cleanup.policy | " compact , delete " | "compact,delete" |
      | cleanup.policy | " " | "" |
      | leader.replication.throttled.replicas | " " | "" |
      | leader.replication.throttled.replicas | " * " | "*" |
      | leader.replication.throttled.replicas | "0:1,,1:2" | "0:1,,1:2" |
      | leader.replication.throttled.replicas | "-1:1" | rejected |
      | leader.replication.throttled.replicas | "+0:1" | rejected |
      | follower.replication.throttled.replicas | "0 : 1" | rejected |
