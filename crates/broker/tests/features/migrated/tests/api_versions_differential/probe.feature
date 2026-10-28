Feature: tests api versions differential probe

  Scenario Outline: tests: a child that exits in time reports its status
    Then api versions differential probe tests: a child that exits in time reports its status for <example>

    Examples:
      | example |
      | success |
      | failure |

  Scenario: tests: a child that outruns its budget is killed and reaped
    Then api versions differential probe tests: a child that outruns its budget is killed and reaped

  Scenario: tests: a zero budget still reports a child that has already exited
    Then api versions differential probe tests: a zero budget still reports a child that has already exited

  Scenario: tests: readiness stops at the first ready probe
    Then api versions differential probe tests: readiness stops at the first ready probe

  Scenario: tests: a probe that blocks cannot outlive the deadline
    Then api versions differential probe tests: a probe that blocks cannot outlive the deadline

  Scenario: tests: a probe that never reports ready gives up at the deadline
    Then api versions differential probe tests: a probe that never reports ready gives up at the deadline
