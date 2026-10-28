Feature: Kafka v1 producer snapshot compatibility
  Scenario: Producer state survives writing and reading a Kafka snapshot
    Given producer snapshot entries
      | producer | epoch | sequence | offset | delta | timestamp | coordinator | transaction |
      | 42       | 3     | 9        | 101    | 4     | 1234567   | 8           | 99          |
    When the Kafka v1 snapshot is written and read at exclusive offset 102
    Then every producer snapshot field survives the Kafka v1 round trip

  Scenario: A marker-only producer keeps Kafka's sentinel state
    Given producer 7 at epoch 0 has only a marker
    When an empty producer snapshot entry is created
    Then the marker-only entry uses Kafka's unknown-offset sequence and timestamp sentinels
