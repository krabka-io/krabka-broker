Feature: Live follower replication removes a divergent suffix beyond the leader's epoch boundary
  Scenario: A running follower removes its divergent suffix and matches the leader
    Given three replicas of topic divtrunc have acknowledged 8 numbered epoch zero records led by broker one
    When a follower's local log receives a divergent suffix of 5 records without a new epoch checkpoint
    Then the divergent follower initially has log end offset 13
    And the running follower converges to its leader at log end offset 8
