Feature: src txn two pc

  Scenario Outline: tests: resolve timeout refuses what kafka refuses
    Then txn two pc tests: resolve timeout refuses what kafka refuses for <example>

    Examples:
      | example       |
      | one           |
      | ordinary      |
      | at maximum    |
      | above maximum |
      | zero          |
      | negative      |
      | minimum       |
      | maximum i32   |
