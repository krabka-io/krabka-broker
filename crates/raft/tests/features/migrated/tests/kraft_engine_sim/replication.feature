Feature: Metadata submissions follow the leader and replicate record bytes
  Scenario: A follower hints the leader and the leader replicates the submitted topic
    Given three running voters have elected a leader for the metadata log
    When a follower receives the orders topic submission
    Then the follower refuses with NotLeader and the current leader hint
    When the hinted leader commits the orders topic and every voter applies it
    Then all three metadata images contain the orders topic
