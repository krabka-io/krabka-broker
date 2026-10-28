Feature: src bin broker runtime args

  Scenario Outline: tests: message max bytes takes kafkas int at least zero
    Then bin broker runtime args tests: message max bytes takes kafkas int at least zero for <example>

    Examples:
      | example         |
      | zero            |
      | bytes           |
      | int max         |
      | mebibytes       |
      | negative        |
      | above int max   |
      | gibibytes       |
      | fractional byte |
