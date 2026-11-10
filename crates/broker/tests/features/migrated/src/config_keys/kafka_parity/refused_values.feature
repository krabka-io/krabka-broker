Feature: Kafka topic value rejection boundaries
  Depends on: Topic refusals preserve Kafka ConfigException text - ../validation/tests/values/exact_refusals.feature

  Scenario Outline: Values refused by Kafka remain refused by the broker
    Given an operator alters topic key <key> to quoted value <value>
    When the released topic alteration validator checks the operator value
    Then the individual topic alteration is refused

    Examples:
      | case                         | key                           | value     |
      | a key Kafka 4 removed        | message.downconversion.enable | "true"    |
      | lz4 level below one          | compression.lz4.level         | "0"       |
      | zstd level below its minimum | compression.zstd.level        | "-131073" |
      | zstd level above its maximum | compression.zstd.level        | "23"      |
