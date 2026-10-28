Feature: src handlers incremental alter configs group scope

  Scenario: tests: group config stores every key kafka 4 3 1 accepts
    Then handlers incremental alter configs group scope tests: group config stores every key kafka 4 3 1 accepts

  Scenario Outline: tests: group config takes every share offset reset strategy kafka accepts
    Then handlers incremental alter configs group scope tests: group config takes every share offset reset strategy kafka accepts for <example>

    Examples:
      | example           |
      | latest            |
      | earliest          |
      | by duration       |
      | negative duration |
      | missing duration  |
      | unknown strategy  |

  Scenario: tests: group resources follow kafkas merge and validation order
    Then handlers incremental alter configs group scope tests: group resources follow kafkas merge and validation order
