Feature: DescribeGroups exposes every classic coordinator state in Kafka vocabulary
  Scenario Outline: Coordinator transitions have distinct public state names
    Given the classic group <condition>
    When DescribeGroups renders the classic group state
    Then the reported classic group state is <expected>
    Examples:
      | condition                  | expected            |
      | has no members             | Empty               |
      | is preparing a rebalance   | PreparingRebalance  |
      | is completing a rebalance  | CompletingRebalance |
      | has a stable assignment    | Stable              |
