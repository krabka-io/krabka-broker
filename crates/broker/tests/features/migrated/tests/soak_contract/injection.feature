Feature: tests soak contract injection

  Scenario Outline: tests: only a record sent once and acknowledged is deleted
    Then soak contract injection tests: only a record sent once and acknowledged is deleted for <example>

    Examples:
      | example                  |
      | acknowledged once        |
      | never acknowledged       |
      | failed then acknowledged |
      | resent                   |
