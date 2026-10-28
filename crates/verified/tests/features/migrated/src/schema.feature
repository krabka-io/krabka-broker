Feature: src schema

  Scenario Outline: tests: only configured non null fields are validated
    Then schema tests: only configured non null fields are validated for <example>

    Examples:
      | example              |
      | key enabled          |
      | value enabled        |
      | both key             |
      | both value           |
      | null key             |
      | null value           |
      | value only skips key |
      | key only skips value |
      | disabled             |
