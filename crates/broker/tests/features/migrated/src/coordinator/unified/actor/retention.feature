Feature: Retention tombstones offsets before deleting empty group metadata
  Scenario: Expired offset without group deletion
    Given the expired t partition 0 offset belongs to group g of kind absent
    When the retention sweep builds the expired offset and empty group tombstone batch
    Then the retention tombstones have these exact Kafka keys in order and every value is null
      | family | identifier |
      | offset | t:0 |
  Scenario: Expired offset and empty classic group
    Given the expired t partition 0 offset belongs to group g of kind classic
    When the retention sweep builds the expired offset and empty group tombstone batch
    Then the retention tombstones have these exact Kafka keys in order and every value is null
      | family | identifier |
      | offset | t:0 |
      | classic-group | g |
  Scenario: Expired offset and empty consumer group
    Given the expired t partition 0 offset belongs to group g of kind consumer
    When the retention sweep builds the expired offset and empty group tombstone batch
    Then the retention tombstones have these exact Kafka keys in order and every value is null
      | family | identifier |
      | offset | t:0 |
      | target-epoch | g |
      | partition-metadata | g |
      | consumer-group | g |
  Scenario: Regex resolutions are removed in sorted order before the consumer group
    Given the expired t partition 0 offset belongs to group g of kind consumer-with-regex
    When the retention sweep builds the expired offset and empty group tombstone batch
    Then the retention tombstones have these exact Kafka keys in order and every value is null
      | family | identifier |
      | offset | t:0 |
      | target-epoch | g |
      | regex | a.* |
      | regex | b.* |
      | partition-metadata | g |
      | consumer-group | g |
