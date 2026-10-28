Feature: src coordinator unified classic state protocol

  Scenario Outline: tests: supports protocols matches kafka
    Then coordinator unified classic state protocol tests: supports protocols matches kafka for <example>

    Examples:
      | example                 |
      | empty empty type        |
      | empty no protocols      |
      | empty other type        |
      | stable other type       |
      | stable missing protocol |
      | stable shared protocol  |
