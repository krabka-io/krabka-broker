Feature: src remote log manager remote retention tests

  Scenario Outline: remote time retention follows kafkas cleanup until
    Then remote log manager remote retention tests: remote time retention follows kafkas cleanup until for <example>

    Examples:
      | example                  |
      | unknown timestamp        |
      | below cutoff             |
      | on cutoff                |
      | window longer than clock |
      | window at clock          |
