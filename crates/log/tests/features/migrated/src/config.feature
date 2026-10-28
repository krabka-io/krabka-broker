Feature: src config

  Scenario: tests: defaults match kafka 4x
    Then config tests: defaults match kafka 4x

  Scenario: tests: defaults cross the raw seams as kafkas documented numbers
    Then config tests: defaults cross the raw seams as kafkas documented numbers

  Scenario Outline: tests: a policy reports the halves of kafkas cleanup policy list it contains
    Then config tests: a policy reports the halves of kafkas cleanup policy list it contains for <example>

    Examples:
      | example            |
      | delete             |
      | compact            |
      | compact and delete |
      | no cleanup         |
