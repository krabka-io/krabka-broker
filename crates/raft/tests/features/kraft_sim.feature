Feature: Deterministic KRaft consensus
  Scenario: Three nodes elect exactly one leader
    Given three in-memory KRaft voters
    When the voters stabilize
    Then all voters agree on one leader epoch

  Scenario: A partitioned leader rejoins as a follower
    Given three in-memory KRaft voters
    And the voters have elected a leader
    When the leader is isolated
    Then the majority elects exactly one replacement leader
    When the partition heals
    Then the replacement remains the only leader

  Scenario: Committed high watermarks agree across voters
    Given three in-memory KRaft voters
    And the voters have elected a leader
    When the leader appends 5 records and replication settles
    Then the leader commits the appended records
    And all voters fetch through the committed high watermark
