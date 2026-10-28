Feature: Parse spaced offsets in JVM log dumps

  Scenario: Read the last offset beyond the batch summary
    Given Kafka dump-log output
      """
      baseOffset: 0 lastOffset: 9 count: 10
      offset: 10 position: 211 payload: value
      """
    When the highest Kafka dump offset is parsed
    Then the highest offset is 10
