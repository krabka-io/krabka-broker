Feature: src sim node

  Scenario Outline: tests: election timeouts land on whole staggered milliseconds
    Then sim node tests: election timeouts land on whole staggered milliseconds for <example>

    Examples:
      | example |
      | first   |
      | second  |
      | third   |
      | seventh |
