Feature: Kafka compatible subscription expression validation
  Scenario: Unsupported extended mode returns the RE2J client error
    Given a member subscribes with regular expression (?x)abc
    When the coordinator validates the expression using Kafka RE2J syntax rules
    Then the unsupported extended mode flag is rejected with the exact Kafka syntax error
