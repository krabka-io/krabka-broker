Feature: src config keys message size

  Scenario Outline: tests: max message bytes matches kafkas int at least zero
    Then config keys message size tests: max message bytes matches kafkas int at least zero for <example>

    Examples:
      | example       |
      | zero          |
      | positive      |
      | i32 max       |
      | negative      |
      | above i32 max |
      | non integer   |
      | empty         |
