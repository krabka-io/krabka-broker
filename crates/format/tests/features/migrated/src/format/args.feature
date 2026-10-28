Feature: src format args

  Scenario Outline: tests: parse node id takes kafkas node id range
    Then format args tests: parse node id takes kafkas node id range for <example>

    Examples:
      | example      |
      | ordinary     |
      | trims spaces |
      | zero         |
      | max i32      |
      | overflow     |
      | negative     |
      | empty        |
      | decimal      |
      | hex          |
