Feature: src log state

  Scenario Outline: tests: a reopened log reports the start kafka reports
    Then log state tests: a reopened log reports the start kafka reports for <example>

    Examples:
      | example                |
      | tiered nobody moved it |
      | tiered moved           |
      | local only             |
