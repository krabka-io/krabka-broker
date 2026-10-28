Feature: Interactive consensus playground controls
  Operator controls advance the real deterministic scheduler and expose the same
  messages, elections and faults on the browser event timeline.

  Scenario: Stepping a fresh cluster elects one leader
    Given a fresh three-voter playground cluster
    When the operator steps the playground through an election
    Then the fresh cluster had three voters and no leader
    And the playground reports one leader and an advancing event timeline

  Scenario: A healed leader partition converges to one leader
    Given the playground has elected a leader
    When the operator isolates the playground leader and heals the partition
    Then the healed playground has exactly one leader

  Scenario: Dropping a message changes the bus and records the fault
    Given election traffic is queued in the playground
    When the operator drops the next playground message
    Then one queued message is removed and one drop event is recorded

  Scenario: Reordering and replay expose a consistent event history
    Given election traffic is queued in the playground
    When the operator reorders election traffic and duplicates the next available message
    Then playground accessors and replay faults report the same event history

  Scenario: Append is routed only to an elected leader
    Given a fresh three-voter playground cluster
    When the operator appends two records without a playground leader
    Then the playground refuses the append without a leader
    When the operator elects a leader and appends two records
    Then the elected playground leader gains exactly two records
