Feature: Reassignment cancellation under two-person approval
  Scenario: reassign partitions cancel reports the break glass gate to the jvm tool
    Given a SASL cluster with admin and operators alice, bob, carol has an offline-target reassignment for krabka-cancel-gate-itest-0
    When the administrator cancels the reassignment while preserving throttles
    Then the cancellation succeeds and reports partition krabka-cancel-gate-itest-0
    When the same cluster restarts on its existing logs with unsigned two-person approvals and starts another offline-target reassignment
    And the administrator cancels the reassignment while preserving throttles
    Then cancellation is refused with this exact policy sentence and the reassignment remains in flight
      """
      break-glass refused cancel_reassignment on krabka-cancel-gate-itest-0: no approved proposal covers the request
      """
    When alice proposes cancel_reassignment and distinct operators bob and carol approve it
    Then one approval is insufficient and the second reaches the required count
    When the administrator cancels the reassignment while preserving throttles
    Then the cancellation succeeds and reports partition krabka-cancel-gate-itest-0
