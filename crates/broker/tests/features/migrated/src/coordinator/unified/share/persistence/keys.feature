Feature: Share group metadata key schema
  Scenario Outline: Record kinds use Kafka compacted-log API keys
    Given the Share compacted log record kind is <kind>
    When the Share coordinator selects the record API key
    Then the Share record API key is <version>
    Examples:
      | kind | version |
      | member-metadata | 10 |
      | group-metadata | 11 |
      | target-assignment-metadata | 12 |
      | target-assignment-member | 13 |
      | current-member-assignment | 14 |
      | group-state-partition-metadata | 15 |
  Scenario Outline: Group and member identifiers use Kafka non-flexible strings
    Given a Share <kind> metadata key for group g1 and member <member>
    When the Share coordinator encodes the metadata key
    Then the Share metadata key has exact Kafka bytes <hex>
    Examples:
      | kind | member | hex |
      | group | absent | 000b00026731 |
      | member | m1 | 000a0002673100026d31 |
