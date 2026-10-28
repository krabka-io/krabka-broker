Feature: Kafka compaction lag consistency
  Minimum compaction lag cannot exceed maximum lag; the refusal retains the
  field names and values Kafka-configs prints to the operator.

  Scenario: A minimum lag above the maximum is refused precisely
    Given the topic alteration contains these properties:
      | key                   | value |
      | min.compaction.lag.ms | 60000 |
      | max.compaction.lag.ms | 1     |
    When the broker validates the resulting topic combination with tiered storage available
    Then the topic combination is refused with exact Kafka message conflict topic config setting min.compaction.lag.ms (60000) > max.compaction.lag.ms (1)
