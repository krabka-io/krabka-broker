Feature: src handlers authorized operations

  Scenario: tests: supported operations topic matches kafka
    Then handlers authorized operations tests: supported operations topic matches kafka

  Scenario: tests: supported operations group matches kafka
    Then handlers authorized operations tests: supported operations group matches kafka

  Scenario: tests: supported operations cluster matches kafka
    Then handlers authorized operations tests: supported operations cluster matches kafka

  Scenario Outline: tests: bit values match kafka int8 codes
    Then handlers authorized operations tests: bit values match kafka int8 codes for <example>

    Examples:
      | example          |
      | read             |
      | write            |
      | create           |
      | delete           |
      | alter            |
      | describe         |
      | cluster action   |
      | describe configs |
      | alter configs    |
      | idempotent write |
