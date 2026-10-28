Feature: src kraft transport wire response tests

  Scenario Outline: a refused fetch is kafkas empty fetch response
    Then kraft transport wire response tests: a refused fetch is kafkas empty fetch response for <example>

    Examples:
      | example        |
      | known leader   |
      | unknown leader |
