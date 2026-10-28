Feature: Registered Kafka topic names retain both acceptance and refusal examples
  Each name is recognized and enforces the same legal and illegal samples as Kafka.

  Scenario Outline: Registered Kafka topic names retain both acceptance and refusal examples
    Given topic key <key> has quoted legal sample <legal> and quoted illegal sample <illegal>
    When the broker recognizes the topic key and validates both named samples
    Then the key is recognized, its legal sample is accepted, and its illegal sample is refused

    Examples:
      | key | legal | illegal |
      | segment.ms | "60000" | "0" |
      | segment.index.bytes | "10485760" | "3" |
      | segment.jitter.ms | "0" | "-1" |
      | min.compaction.lag.ms | "0" | "-1" |
      | max.compaction.lag.ms | "9223372036854775807" | "0" |
      | min.cleanable.dirty.ratio | "0.5" | "2.0" |
      | file.delete.delay.ms | "60000" | "-1" |
      | flush.messages | "10000" | "0" |
      | flush.ms | "1000" | "-1" |
      | index.interval.bytes | "4096" | "-1" |
      | preallocate | "true" | "yes" |
      | message.timestamp.type | "LogAppendTime" | "WallClock" |
      | message.timestamp.after.max.ms | "3600000" | "-1" |
      | message.timestamp.before.max.ms | "3600000" | "-1" |
