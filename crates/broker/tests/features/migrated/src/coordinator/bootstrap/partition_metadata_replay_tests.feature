Feature: Bootstrap loads deprecated Kafka consumer subscription metadata
  Scenario Outline: Partition metadata creates and marks groups while tombstones clear its presence
    Given the persisted consumer partition metadata log contains <records>
    When bootstrap loads the deprecated consumer partition metadata records
    Then the consumer metadata load is <outcome> at group epoch <epoch> with persisted subscription marker <marked>
    Examples:
      | records | outcome | epoch | marked |
      | metadata | group | 1 | true |
      | group-epoch-4,metadata | group | 4 | true |
      | group-epoch-4,metadata,metadata-tombstone | group | 4 | false |
      | metadata-tombstone | absent | 0 | false |
      | unknown-version | failed | 0 | false |
      | truncated | failed | 0 | false |
