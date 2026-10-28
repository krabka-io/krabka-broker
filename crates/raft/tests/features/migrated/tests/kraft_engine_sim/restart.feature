Feature: Restart recovers committed metadata
  Scenario: A follower rebuilds its image from checkpoint and log
    Given three voters have committed and replicated the persistent topic
    When a follower snapshots its image and restarts from its own directory
    Then the reopened follower immediately recovers the persistent topic
