Feature: src leader epoch checkpoint lookup

  Scenario Outline: tests: epoch and offset for follows kafka end offset for
    Then leader epoch checkpoint lookup tests: epoch and offset for follows kafka end offset for for <example>

    Examples:
      | example     |
      | empty       |
      | undefined   |
      | latest      |
      | oldest      |
      | middle      |
      | gap         |
      | future      |
      | below first |
