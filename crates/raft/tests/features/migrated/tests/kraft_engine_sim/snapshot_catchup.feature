Feature: Kafka snapshot catch-up and checkpoint retention
  Scenario: An empty voter installs a snapshot and converges
    Given two voters in cluster 500 have elected a leader and snapshot every five records
    When the leader commits fifteen topics prefixed t beginning at identity 1000
    Then the leader has pruned its log and contains all fifteen topics prefixed t
    When voter 3 joins with an empty log and catches up to the leader image
    Then voter 3 has the complete leader image and a nonzero snapshot log boundary
  Scenario: A pruned follower redirects Fetch and FetchSnapshot to the leader
    Given two voters in cluster 501 have elected a leader and snapshot every five records
    When the leader commits fifteen topics prefixed f beginning at identity 2000
    And the follower finishes independent pruning and receives a Fetch from offset zero
    Then the follower retains one or two checkpoints and refuses Fetch with the current leader and its own log start
    When the lagging peer asks the follower for its latest checkpoint
    Then the follower refuses FetchSnapshot with NotLeaderOrFollower and leader 1 at the current epoch
  Scenario: A snapshot transfer survives a leader checkpoint roll
    Given two voters in cluster 502 have elected a leader and snapshot every five records
    When the leader commits fifteen topics prefixed r beginning at identity 3000
    And a lagging peer starts a snapshot transfer with only the header batch
    Then the snapshot transfer is incomplete and every chunk retains its original identity without error
    When newly committed topics roll the leader onto a newer checkpoint
    Then the leader checkpoint catalogue has changed
    When the peer finishes the original snapshot transfer against the rolled leader
    Then all chunks preserve the original snapshot identity and decode only the pre-roll topics
