Feature: Replication throttles preserve progress and ISR availability

  Scenario: throttle rate caps fetch response size
    Given a replication leader with follower 2 whose ISR membership is false
    And the leader rate is 512 bytes per second and the throttled replica list names the leader
    And the replicated partition contains eight 1024-byte records
    When follower 2 fetches the replicated partition
    Then the first replica response contains at least 4096 bytes
    When another eight 1024-byte records are appended before follower 2 fetches again
    Then the second replica response contains at most 2048 bytes of framing and records

  Scenario: unthrottled partition unaffected
    Given a replication leader with follower 2 whose ISR membership is false
    And the replicated partition contains eight 1024-byte records
    When follower 2 fetches the replicated partition
    Then the first replica response contains at least 4096 bytes

  Scenario: a list naming only the follower throttles nothing on the leader
    Given a replication leader with follower 2 whose ISR membership is false
    And the leader rate is 512 bytes per second and the throttled replica list names only follower 2
    And the replicated partition contains eight 1024-byte records
    When follower 2 fetches the replicated partition
    Then the first replica response contains at least 4096 bytes

  Scenario: a follower in the isr is not throttled
    Given a replication leader with follower 2 whose ISR membership is true
    And the leader rate is 512 bytes per second and the throttled replica list names every replica
    And the replicated partition contains eight 1024-byte records
    When follower 2 fetches the replicated partition
    Then the first replica response contains at least 4096 bytes
