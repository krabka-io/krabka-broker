Feature: tests partition reassignment reassign lifecycle

  Scenario: preplacement log dir preference survives until reassignment materializes the replica
    Then partition reassignment reassign lifecycle: preplacement log dir preference survives until reassignment materializes the replica

  Scenario: alter then complete via isr catchup
    Then partition reassignment reassign lifecycle: alter then complete via isr catchup

  Scenario: list in flight returns pending rows
    Then partition reassignment reassign lifecycle: list in flight returns pending rows

  Scenario: cancel via null replicas reverts
    Then partition reassignment reassign lifecycle: cancel via null replicas reverts
