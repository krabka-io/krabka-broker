Feature: Live follower directory failure
  @allow_skipped
  Scenario: A follower reports a failed directory during live replication
    Given three brokers with two log directories and a 100 ms segment roll
    And a fully replicated target partition placed in a follower's second log directory
    When the follower's second directory becomes read-only and the next append rolls its segment
    Then the follower leaves the ISR, retains only its healthy directory, and stays alive without changing the leader
