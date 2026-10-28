Feature: Metadata event consumers honor partition assignment, offsets, keys, and per-partition order

  Background:
    Given a bare broker hosts a metadata event log with at least three partitions

  Scenario: subscribe subset from nonzero offset yields exact records
    When these unkeyed metadata records are published in order
      | partition | payload |
      | 0         | a       |
      | 0         | b       |
      | 0         | c       |
      | 1         | x       |
      | 1         | y       |
      | 2         | z       |
    And a metadata consumer subscribes at these partition offsets
      | partition | offset |
      | 0         | 1      |
      | 1         | 0      |
    And the metadata consumer reads 4 records within fifteen seconds
    Then the assigned metadata values are exactly
      | partition | offset | payload |
      | 0         | 1      | b       |
      | 0         | 2      | c       |
      | 1         | 0      | x       |
      | 1         | 1      | y       |
    When the metadata consumer observes another 500-millisecond grace window
    Then no unassigned partition-two record arrived in the grace window

  Scenario: keyed group lands each partition in order
    When an empty keyed metadata group is published
    Then the empty metadata group receives no offsets
    When this interleaved keyed metadata group is published
      | partition | key | value     |
      | 0         | k-a | a         |
      | 1         | k-x | x         |
      | 0         | k-b | b         |
      | 2         | k-z | z         |
      | 1         | k-y | y         |
      | 0         | k-a | tombstone |
    Then the metadata group offsets are 0,0,1,0,1,2
    When a metadata consumer subscribes at these partition offsets
      | partition | offset |
      | 0         | 0      |
      | 1         | 0      |
      | 2         | 0      |
    And the metadata consumer reads 6 records within fifteen seconds
    Then the assigned keyed metadata records are exactly
      | partition | offset | key | payload | tombstone |
      | 0         | 0      | k-a | a       | false     |
      | 0         | 1      | k-b | b       | false     |
      | 0         | 2      | k-a | absent  | true      |
      | 1         | 0      | k-x | x       | false     |
      | 1         | 1      | k-y | y       | false     |
      | 2         | 0      | k-z | z       | false     |
