Feature: Interactive consensus playground controls

  Rule: A three-voter cluster elects one leader
    Scenario: Stepping a fresh cluster elects one leader
      Given a fresh three-voter playground cluster
      When the operator steps the playground through an election
      Then the fresh cluster starts leaderless with three voters
      And the playground records the elected leader on its event timeline

  Rule: A connected majority replaces an isolated leader
    Scenario: A partitioned leader loses leadership to the majority
      Given the playground has elected a leader
      When the operator isolates the playground leader
      Then the connected majority elects one replacement at a higher epoch

    Scenario: A healed partition retains the replacement leader
      Given the connected majority has replaced an isolated playground leader
      When the operator heals the playground partition
      Then every healed voter reaches the replacement leader's epoch

  Rule: Dropping a message removes one delivery
    Scenario: Dropping a message changes the bus and records the fault
      Given election traffic is queued in the playground
      When the operator drops the next playground message
      Then the event timeline records the removal of one queued message

  Rule: Reordering reverses queued delivery order
    Scenario: Reordering delivers the queued messages back to front
      Given election traffic is queued in the playground
      When the operator reorders the queued election traffic
      Then the event timeline delivers every previously queued message in reverse order

  Rule: Duplication repeats exactly one delivery
    Scenario: Duplicating delivers the same message exactly twice
      Given election traffic is queued in the playground
      When the operator duplicates the next playground message
      Then the event timeline records exactly two deliveries of the next queued message

  Rule: An append requires an elected leader
    Scenario: Append without an elected leader is refused
      Given a fresh three-voter playground cluster
      When the operator appends two records without a playground leader
      Then the playground refuses the append without a leader

    Scenario: The elected leader receives appended records
      Given the playground has elected a leader
      When the operator appends two records
      Then the elected playground leader gains exactly two records
