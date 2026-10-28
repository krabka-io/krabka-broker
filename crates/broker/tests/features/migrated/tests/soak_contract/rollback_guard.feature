Feature: tests soak contract rollback guard

  Scenario Outline: tests: only what an open rebuilds may change
    Then soak contract rollback guard tests: only what an open rebuilds may change for <example>

    Examples:
      | example              |
      | untouched            |
      | snapshot removed     |
      | time index rebuilt   |
      | offset index trimmed |
      | checkpoint rewritten |
      | segment appended     |
      | quorum state written |

  Scenario: tests: a directory is read file by file
    Then soak contract rollback guard tests: a directory is read file by file

  Scenario: tests: the refusal is kafkas with the sources range
    Then soak contract rollback guard tests: the refusal is kafkas with the sources range
