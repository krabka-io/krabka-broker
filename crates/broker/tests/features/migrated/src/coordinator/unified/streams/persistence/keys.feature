Feature: Streams group metadata key schema
  Scenario Outline: Record kinds use Kafka compacted-log API keys
    Given the Streams compacted log record kind is <kind>
    When the Streams coordinator selects the record API key
    Then the Streams record API key is <version>
    Examples:
      | kind | version |
      | group-metadata | 17 |
      | member-metadata | 19 |
      | target-assignment-metadata | 20 |
      | target-assignment-member | 21 |
      | current-member-assignment | 22 |
      | topology | 23 |
  Scenario Outline: Group and member identifiers use Kafka non-flexible strings
    Given a Streams <kind> metadata key for group g1 and member <member>
    When the Streams coordinator encodes the metadata key
    Then the Streams metadata key has exact Kafka bytes <hex>
    Examples:
      | kind | member | hex |
      | group | absent | 001100026731 |
      | member | m1 | 00130002673100026d31 |
