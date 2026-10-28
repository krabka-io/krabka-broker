Feature: Consumer partition metadata codec compatibility
  Scenario: Topic partition racks use the Kafka metadata layout
    Given consumer partition metadata contains the Kafka golden topic and partition rack assignments
    When the coordinator writes the partition metadata and reads Kafka golden bytes
    Then the partition metadata encodes exactly as Kafka and recovers every topic partition and rack field
  Scenario Outline: Reject unsupported and incomplete partition metadata records
    Given the consumer partition metadata bytes have corruption <corruption>
    When the coordinator reads the consumer partition metadata bytes
    Then Kafka incompatible partition metadata is rejected
    Examples:
      | corruption |
      | unknown-value-version |
      | missing-trailer |
      | empty |
