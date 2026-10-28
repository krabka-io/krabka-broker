Feature: src manager rlmm

  Scenario Outline: tests: caught up answers from the current high water mark
    Then manager rlmm tests: caught up answers from the current high water mark for <example>

    Examples:
      | example     |
      | seeded      |
      | behind      |
      | unassigned  |
      | not ready   |
      | hwm failure |
      | shutdown    |
