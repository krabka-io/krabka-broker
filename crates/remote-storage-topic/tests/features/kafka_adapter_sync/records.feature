Feature: Kafka metadata adapter requests
  Scenario: Metadata discovery cannot accidentally create the metadata topic
    Given the metadata request scope contains only the remote metadata topic
    When a metadata discovery request is built
    Then discovery names only the remote metadata topic and forbids automatic creation

  Scenario: A keyed tombstone retains its routing and deletion semantics
    Given a tombstone for key range in partition 2 of the three-partition WAL index
    When the producer record is built
    Then the producer preserves its WAL topic partition key and null value

  Scenario Outline: Records must target an existing partition
    Given a record targets partition <partition> of the three-partition index
    When the producer record is built
    Then the requested partition is rejected against a partition count of 3
    Examples:
      | partition |
      | -1        |
      | 3         |
