Feature: Effective minimum ISR resolution
  A topic override wins over the cluster default and Kafka's default of one.
  Invalid stored values fall back, and the replication factor caps the result.

  Scenario Outline: Minimum ISR follows override precedence and cannot exceed replication
    Given topic min.insync.replicas is <topic> and the cluster default is <cluster>
    And the topic's replication factor is <replicas>
    When the broker resolves the topic's effective minimum ISR
    Then the effective minimum ISR is <minimum>

    Examples:
      | topic | cluster | replicas | minimum |
      | unset | unset   | 3        | 1       |
      | unset | 2       | 3        | 2       |
      | 3     | 2       | 3        | 3       |
      | many  | unset   | 3        | 1       |
      | 5     | unset   | 3        | 3       |
      | unset | 9       | 2        | 2       |
