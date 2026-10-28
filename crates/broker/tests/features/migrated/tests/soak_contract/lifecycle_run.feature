Feature: tests soak contract lifecycle run

  Scenario Outline: tests: a configuration holds what was set
    Then soak contract lifecycle run tests: a configuration holds what was set for <example>

    Examples:
      | example |
      | held    |
      | changed |
      | hidden  |
      | missing |
