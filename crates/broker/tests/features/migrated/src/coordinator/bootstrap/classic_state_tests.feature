Feature: Bootstrap reconstructs classic group protocol membership
  Scenario Outline: Stored member rebalance timeout uses the session timeout only when omitted
    Given a persisted classic consumer member has instance <instance> rebalance timeout <rebalance> and session timeout <session> milliseconds
    When bootstrap applies the classic group metadata record
    Then the loaded classic group preserves all stored member and protocol fields with Kafka timeout defaults
    Examples:
      | instance | rebalance | session |
      | inst | 45000 | 30000 |
      | absent | -1 | 30000 |
  Scenario: An empty stored protocol type becomes absent
    Given persisted classic group g is empty with an empty protocol type and generation 5
    When bootstrap applies the classic group metadata record
    Then the loaded classic group preserves all stored member and protocol fields with Kafka timeout defaults
