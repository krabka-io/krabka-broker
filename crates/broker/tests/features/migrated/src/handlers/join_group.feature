Feature: src handlers join group

  Scenario Outline: tests: request checks match kafka defaults
    Then handlers join group tests: request checks match kafka defaults for <example>

    Examples:
      | example          |
      | empty group      |
      | negative timeout |
      | below minimum    |
      | at minimum       |
      | at maximum       |
      | above maximum    |

  Scenario: tests: protocol name normalised like kafka
    Then handlers join group tests: protocol name normalised like kafka
