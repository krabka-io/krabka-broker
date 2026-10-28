Feature: tests share groups share group state

  Scenario: lifecycle initializes share state
    Then share groups share group state: lifecycle initializes share state

  Scenario: lifecycle metadata survives restart
    Then share groups share group state: lifecycle metadata survives restart

  Scenario: delete groups deletes an empty share group and its state
    Then share groups share group state: delete groups deletes an empty share group and its state

  Scenario: a deleted topic loses its share state
    Then share groups share group state: a deleted topic loses its share state

  Scenario: unassigned partitions keep their share state
    Then share groups share group state: unassigned partitions keep their share state

  Scenario: every initialized partition starts uninitialized
    Then share groups share group state: every initialized partition starts uninitialized
