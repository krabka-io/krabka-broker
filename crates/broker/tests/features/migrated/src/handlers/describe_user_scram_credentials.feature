Feature: src handlers describe user scram credentials

  Scenario: tests: credential infos are ordered by kafka scram mechanism type
    Then handlers describe user scram credentials tests: credential infos are ordered by kafka scram mechanism type

  Scenario Outline: tests: denied response matches kafka get error response
    Then handlers describe user scram credentials tests: denied response matches kafka get error response for <example>

    Examples:
      | example        |
      | absent users   |
      | empty users    |
      | repeated users |
