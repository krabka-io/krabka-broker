Feature: src kafka uuid

  Scenario Outline: tests: refuses non newtypes and generic wrappers
    Then kafka uuid tests: refuses non newtypes and generic wrappers for <example>

    Examples:
      | example               |
      | unit struct           |
      | empty named struct    |
      | named field           |
      | multiple tuple fields |
      | const generic         |

  Scenario: tests: emits the four trait implementations for a single field wrapper
    Then kafka uuid tests: emits the four trait implementations for a single field wrapper
