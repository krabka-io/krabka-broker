Feature: src leader epoch

  Scenario Outline: tests: lookup matches kafka end offset for rows
    Then leader epoch tests: lookup matches kafka end offset for rows for <example>

    Examples:
      | example                   |
      | empty cache               |
      | undefined requested empty |
      | undefined requested       |
      | latest epoch              |
      | above latest              |
      | below first               |
      | exact older               |
      | gap epoch                 |
      | gap below latest          |
      | gap above first           |
