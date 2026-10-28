Feature: DescribeTransactions preserves per-ID coordinator state and failure semantics
  Scenario: Each requested transaction receives its own Kafka state row
    Given this broker coordinates an ongoing transaction a dead transaction and an absent ID but not a foreign ID
    When an admin describes the empty absent foreign dead and ongoing transaction IDs in that order
    Then the transaction rows preserve invalid absent foreign dead and complete ongoing state semantics
