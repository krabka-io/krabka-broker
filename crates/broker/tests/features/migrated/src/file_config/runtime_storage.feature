Feature: src file config runtime storage

  Scenario Outline: tests: message max bytes takes kafkas int at least zero
    Then file config runtime storage tests: message max bytes takes kafkas int at least zero for <example>

    Examples:
      | example         |
      | zero            |
      | kibibytes       |
      | max i32         |
      | negative        |
      | above i32       |
      | gibibytes       |
      | fractional byte |

  Scenario: tests: omitted message max bytes keeps kafkas 1048588
    Then file config runtime storage tests: omitted message max bytes keeps kafkas 1048588
