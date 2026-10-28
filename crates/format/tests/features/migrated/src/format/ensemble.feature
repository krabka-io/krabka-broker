Feature: src format ensemble

  Scenario Outline: tests: verify matches kafka meta properties ensemble
    Then format ensemble tests: verify matches kafka meta properties ensemble for <example>

    Examples:
      | example                 |
      | nothing formatted       |
      | no records given id     |
      | set supplies id         |
      | given id disagrees      |
      | another nodes directory |
