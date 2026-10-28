Feature: tests kraft engine sim snapshot catchup

  Scenario: lagging follower catches up via snapshot
    Then kraft engine sim snapshot catchup: lagging follower catches up via snapshot

  Scenario: follower that pruned independently redirects a lagging fetch to the leader
    Then kraft engine sim snapshot catchup: follower that pruned independently redirects a lagging fetch to the leader

  Scenario: a snapshot fetch in flight survives the leader rolling to a new checkpoint
    Then kraft engine sim snapshot catchup: a snapshot fetch in flight survives the leader rolling to a new checkpoint
