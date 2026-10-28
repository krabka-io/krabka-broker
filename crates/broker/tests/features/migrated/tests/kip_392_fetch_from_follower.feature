Feature: Rack-aware committed reads
  Scenario: Rack-aware consumers follow the same-rack replica recommendation
    Given a rack-aware leader in rack-a and in-sync follower in rack-b
    And 5 records committed with all replica acknowledgements
    When the consumer fetches from the leader using rack rack-b
    Then partition 0 recommends read replica 2
    When the rack-b consumer fetches from follower 2 until its committed watermark catches up
    Then the follower returns exactly 5 committed records without fetch errors
    When the consumer fetches from the leader using rack rack-a
    Then partition 0 recommends read replica -1
