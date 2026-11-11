Feature: Kafka v1 producer snapshot compatibility
  Scenario: Producer state survives writing and reading a Kafka snapshot
    Given producer snapshot entries
      | producer | epoch | sequence | offset | delta | timestamp | coordinator | transaction |
      | 42       | 3     | 9        | 101    | 4     | 1234567   | 8           | 99          |
      | 9        | 2     | 3        | 100    | 0     | 999999    | 0           | none        |
    When the Kafka v1 snapshot is written and read at exclusive offset 102
    Then every producer snapshot field survives the Kafka v1 round trip

  Scenario Outline: A marker-only producer keeps its identity and Kafka's sentinel state
    Given producer <producer> at epoch <epoch> has only a marker
    When an empty producer snapshot entry is created
    Then the marker-only entry uses Kafka's unknown-offset sequence and timestamp sentinels

    Examples:
      | case                            | producer | epoch |
      | an initial producer epoch       | 7        | 0     |
      | a different producer and epoch  | 42       | 3     |
      | producer zero at the last epoch | 0        | 32767 |
