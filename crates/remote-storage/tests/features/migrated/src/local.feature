Feature: src local

  Scenario Outline: tests: copied files use kafka local tiered storage layout
    Then local tests: copied files use kafka local tiered storage layout for <example>

    Examples:
      | example                 |
      | log                     |
      | offset index            |
      | time index              |
      | snapshot                |
      | leader epoch checkpoint |
      | transaction index       |
