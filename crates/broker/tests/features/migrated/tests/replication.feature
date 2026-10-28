Feature: tests replication

  Scenario: replication factor three propagates to all followers
    Then replication: replication factor three propagates to all followers

  Scenario: out of range at a batch boundary restarts at the leader log start
    Then replication: out of range at a batch boundary restarts at the leader log start

  Scenario: out of range inside a batch restarts at the batch base
    Then replication: out of range inside a batch restarts at the batch base

  Scenario: a follower that catches up across segments copies every batch
    Then replication: a follower that catches up across segments copies every batch

  Scenario: delete records moves every replica log start before it answers
    Then replication: delete records moves every replica log start before it answers

  Scenario: a follower batches every partition of one leader into one session
    Then replication: a follower batches every partition of one leader into one session

  Scenario: a caught up follower fetches a few times a second whatever it follows
    Then replication: a caught up follower fetches a few times a second whatever it follows
