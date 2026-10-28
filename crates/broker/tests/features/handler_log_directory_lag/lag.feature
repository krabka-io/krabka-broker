Feature: DescribeLogDirs reports Kafka current and future offset lag
  Scenario Outline: Current lag is unknown without a local log and otherwise clamps at zero
    Given the current local log for topic t partition 0 has five records
    When the current log lag is requested for topic <topic> partition 0
    Then the log lag is <expected> and the current log end remains 5
    Examples:
      | topic | expected |
      | ghost | -1       |
      | t     | 0        |

  Scenario Outline: Future lag compares current and future ends without clamping
    Given the current local log for topic t partition 3 has five records
    And the future log for topic <topic> partition 3 has <records> records
    When the future log lag is requested for topic <topic> partition 3
    Then the log lag is <expected> and the current log end remains 5
    Examples:
      | topic | records | expected |
      | ghost | 2       | -1       |
      | t     | 2       | 3        |
      | t     | 7       | -2       |
