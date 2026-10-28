Feature: Streams group deletion and cold downgrade persist Kafka record sequences
  Scenario Outline: Delete member records before group and topology metadata
    Given Streams group g has member identifiers <members>
    When the coordinator tombstones the Streams group at timestamp 123
    Then the Streams deletion records follow Kafka current target metadata member group topology order with null values
    Examples:
      | members |
      | none |
      | m1 |
  Scenario: A drained Streams group cold converts to classic atomically
    Given Streams group g has member identifiers none
    When the coordinator cold converts the drained Streams group to classic at timestamp 123
    Then the conversion tombstones Streams metadata and appends one empty classic group in the same timestamped batch
