Feature: src throttle mod

  Scenario Outline: tests: kafka list spellings parse as kafka reads them
    Then throttle mod tests: kafka list spellings parse as kafka reads them for <example>

    Examples:
      | example                    |
      | whitespace means none      |
      | trimmed wildcard means all |
      | spaced pairs               |
      | empty segments are ignored |
