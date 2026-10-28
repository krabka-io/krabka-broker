@docker
Feature: Cross-client console record and timestamp workflows
  Scenario: Stock console tools round trip partition records without a consumer group
    Given console topic krabka-broker-itest has one partition
    And the producer record values are
      | value   |
      | alpha   |
      | bravo   |
      | charlie |
    When the stock console producer writes those values
    Then the stock producer exits successfully
    When a partition console consumer reads the values within 10000 milliseconds
    Then the console consumer emits exactly the producer values in order

  Scenario: The stock console consumer reads an idempotent Rust producer's LZ4 records
    Given host topic krabka-rust-producer-itest is ready for an idempotent Rust LZ4 producer
    And the producer record values are
      | value |
      | x     |
      | y     |
      | z     |
    When the idempotent Rust LZ4 producer writes and flushes those values
    Then every Rust producer acknowledgement names partition zero
    When a partition console consumer reads the values within 20000 milliseconds
    Then the console consumer emits exactly the producer values in order

  Scenario: A log-append timestamp printed by the JVM client resolves back to its record
    Given topic krabka-log-append-time stores broker log-append timestamps
    And the producer record values are
      | value   |
      | stamped |
    When the stock console producer writes those values
    Then the stock producer exits successfully
    When the console consumer prints one record with its timestamp
    Then the printed record carries a numeric LogAppendTime stamp
    When GetOffsetShell resolves that printed timestamp
    Then the printed timestamp resolves to partition zero offset zero
