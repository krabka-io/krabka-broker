Feature: Kafka topic value rejection boundaries
  Compatibility includes Kafka's refusals, not merely its key roster: unknown
  names, invalid codec bounds, negative batch limits, and invalid LONGs stay invalid.

  Scenario Outline: Values refused by Kafka remain refused by the broker
    Given an operator alters topic key <key> to quoted value <value>
    When the released topic alteration validator checks the operator value
    Then the individual topic alteration is refused

    Examples:
      | key | value |
      | message.downconversion.enable | "true" |
      | totally.made.up | "1" |
      | compression.gzip.level | "0" |
      | compression.lz4.level | "18" |
      | compression.lz4.level | "0" |
      | compression.zstd.level | "-131073" |
      | compression.zstd.level | "23" |
      | max.message.bytes | "-1" |
      | segment.bytes | "1048575" |
      | segment.bytes | "0" |
      | compression.type | "none" |
      | retention.bytes | "9223372036854775808" |
