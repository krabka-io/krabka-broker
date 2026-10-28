Feature: Bound readiness probes and child processes by their deadline
  Scenario Outline: tests: a child that exits in time reports its status
    Given a child process executing <script>
    When the child is waited on with a 30000 millisecond budget
    Then the child reports successful exit <successful>
    Examples:
      | script | successful |
      | exit 0 | true |
      | exit 3 | false |

  Scenario: tests: a child that outruns its budget is killed and reaped
    Given a child process executing sleep 300
    When the child is waited on with a 200 millisecond budget
    Then the overrun child is killed and reaped within 30 seconds

  Scenario: tests: a zero budget still reports a child that has already exited
    Given a child process executing exit 0
    And the child has already exited
    When the child is waited on with a 0 millisecond budget
    Then the child reports successful exit true

  Scenario: tests: readiness stops at the first ready probe
    Given a readiness probe that becomes ready on attempt 3
    When readiness is retried for 30000 milliseconds with a 0 millisecond gap
    Then readiness succeeds after exactly 3 attempts

  Scenario: tests: a probe that blocks cannot outlive the deadline
    Given a readiness probe that never becomes ready
    And each probe blocks for all its remaining budget
    When readiness is retried for 300 milliseconds with a 10 millisecond gap
    Then the blocking probe exhausts its budget within ten budgets

  Scenario: tests: a probe that never reports ready gives up at the deadline
    Given a readiness probe that never becomes ready
    When readiness is retried for 200 milliseconds with a 20 millisecond gap
    Then readiness fails at the deadline within 30 seconds
