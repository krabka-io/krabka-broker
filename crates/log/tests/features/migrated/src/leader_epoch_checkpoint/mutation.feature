Feature: src leader epoch checkpoint mutation

  Scenario Outline: tests: assign follows kafka leader epoch file cache assign
    Then leader epoch checkpoint mutation tests: assign follows kafka leader epoch file cache assign for <example>

    Examples:
      | example              |
      | first                |
      | appends              |
      | later same epoch     |
      | same offset          |
      | earlier offset       |
      | same start new epoch |
      | older epoch          |

  Scenario Outline: tests: truncate from start follows kafka leader epoch file cache
    Then leader epoch checkpoint mutation tests: truncate from start follows kafka leader epoch file cache for <example>

    Examples:
      | example      |
      | empty cache  |
      | below        |
      | on first     |
      | inside first |
      | on later     |
      | past all     |
