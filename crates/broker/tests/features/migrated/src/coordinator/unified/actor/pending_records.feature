Feature: Atomic consumer transition records follow Kafka replay dependencies

  # Value literals include the Kafka schema version and tagged-field trailers.
  # Subscription values: no instance or rack, client c, host h, no topics/regex/assignor/classic metadata, timeout 1 ms.
  # Current values: epoch 2, previous epoch 1, stable, no assigned or pending partitions.
  # Group and target metadata: epoch 2, zero hash/timestamp. Targets contain no partitions.

  Scenario: A departing member is removed before subscription and assignment records
    Given the pending consumer delta for group g contains these record values in chained segments
      | segment | family | member | value_hex |
      | 0 | group-epoch |  | 00000000000200 |
      | 0 | subscription | stays | 00000000026302680100000000010000 |
      | 0 | subscription | leaves | tombstone |
      | 0 | target-epoch |  | 00000000000200 |
      | 0 | target | leaves | tombstone |
      | 0 | target | stays | 00000100 |
      | 0 | current | stays | 0000000000020000000100010100 |
      | 0 | current | leaves | tombstone |
    When the coordinator encodes the complete transition as one compacted group log batch
    Then the compacted batch contains exactly these keys and tombstones in Kafka replay order
      | family | member | tombstone |
      | current | leaves | true |
      | target | leaves | true |
      | subscription | leaves | true |
      | subscription | stays | false |
      | group-epoch |  | false |
      | target | stays | false |
      | target-epoch |  | false |
      | current | stays | false |

  Scenario: An upgrade removes classic metadata before writing consumer records
    Given the pending consumer delta for group g contains these record values in chained segments
      | segment | family | member | value_hex |
      | 0 | classic-group |  | tombstone |
      | 0 | group-epoch |  | 00000000000200 |
      | 0 | subscription | m | 00000000026302680100000000010000 |
      | 0 | target-epoch |  | 00000000000200 |
      | 0 | target | m | 00000100 |
      | 0 | current | m | 0000000000020000000100010100 |
    When the coordinator encodes the complete transition as one compacted group log batch
    Then the compacted batch contains exactly these keys and tombstones in Kafka replay order
      | family | member | tombstone |
      | classic-group |  | true |
      | subscription | m | false |
      | group-epoch |  | false |
      | target | m | false |
      | target-epoch |  | false |
      | current | m | false |

  Scenario: Static replacement records precede the following heartbeat delta
    Given the pending consumer delta for group g contains these record values in chained segments
      | segment | family | member | value_hex |
      | 0 | subscription | leaves | tombstone |
      | 0 | subscription | stays | 00000000026302680100000000010000 |
      | 0 | target | leaves | tombstone |
      | 0 | target | stays | 00000100 |
      | 0 | current | leaves | tombstone |
      | 0 | current | stays | 0000000000020000000100010100 |
      | 1 | subscription | stays | 00000000026302680100000000010000 |
      | 1 | current | stays | 0000000000020000000100010100 |
    When the coordinator encodes the complete transition as one compacted group log batch
    Then the compacted batch contains exactly these keys and tombstones in Kafka replay order
      | family | member | tombstone |
      | current | leaves | true |
      | target | leaves | true |
      | subscription | leaves | true |
      | subscription | stays | false |
      | target | stays | false |
      | current | stays | false |
      | subscription | stays | false |
      | current | stays | false |

  Scenario: A metadata refresh without an epoch bump only removes deprecated subscription metadata
    Given the pending consumer delta for group g contains these record values in chained segments
      | segment | family | member | value_hex |
      | 0 | subscription-metadata |  | tombstone |
    When the coordinator encodes the complete transition as one compacted group log batch
    Then the compacted batch contains exactly these keys and tombstones in Kafka replay order
      | family | member | tombstone |
      | subscription-metadata |  | true |
