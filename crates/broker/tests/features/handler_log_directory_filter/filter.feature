Feature: DescribeLogDirs filters the union of explicitly requested partitions
  Scenario Outline: Null topics mean all partitions while explicit lists limit the result
    Given the log directory request lists <requested>
    When the log directory filter checks topic <topic> partition <partition>
    Then that log partition is included <expected>
    Examples:
      | requested  | topic | partition | expected |
      | all topics | any   | 9         | true     |
      | t:0,2      | t     | 2         | true     |
      | t:0,2      | t     | 1         | false    |
      | t:0        | u     | 0         | false    |
      | t:none     | t     | 0         | false    |
      | t:0;t:1    | t     | 0         | true     |
