Feature: A feature containing only rule scenarios
  Rule: A rule can contain the first scenario
    Scenario: No top-level scenario is required
      When the count increases by 3
      Then the count is 3
