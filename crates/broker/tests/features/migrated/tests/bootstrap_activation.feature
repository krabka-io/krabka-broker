Feature: Active controller records formatted bootstrap features once for every node
  Scenario Outline: the active controller writes the bootstrap records once
    Given formatted <quorum_name> quorum nodes with topology <topology> ELR <elr> and static minimum ISR <minimum>
    When the formatted controllers activate before the brokers join
    And a client reads the active controller committed metadata log
    Then bootstrap features occur exactly once in the first metadata batch after the leader control batch
    And every controller and broker image converges to those feature levels and cluster minimum ISR
    When a broker describes its cluster default minimum ISR configuration
    Then DescribeConfigs matches Kafka cluster minimum ISR value source type and synonyms
    Examples:
      | quorum_name | topology | elr | minimum |
      | dynamic | one-controller-two-brokers | enabled | 2 |
      | static | three-controllers-two-brokers | enabled | 1 |
      | static | one-combined-node | enabled | 2 |
      | static | one-combined-node | disabled | 2 |
