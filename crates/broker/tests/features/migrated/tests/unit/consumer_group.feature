Feature: Classic Kafka consumer group lifecycle
  Scenario: join group with empty member returns member id required
    Given a single broker ready to coordinate classic group g
    And the classic rebalance timeout is 2000 milliseconds
    When a consumer joins group g without a member id
    Then the broker requires the newly assigned nonempty member id

  Scenario: join group single member completes after deadline
    Given a single broker ready to coordinate classic group g
    When a consumer joins group g without a member id
    Then the broker requires the newly assigned nonempty member id
    When the consumer rejoins with its assigned member id
    Then the single consumer becomes leader and sees itself in the member list

  Scenario: full group flow join sync heartbeat commit fetch leave
    Given a single broker ready to coordinate classic group g
    And group offsets refer to an existing single-partition topic t
    When a consumer joins group g without a member id
    Then the broker requires the newly assigned nonempty member id
    When the consumer rejoins with its assigned member id
    Then the single consumer becomes leader and sees itself in the member list
    When the group leader syncs assignment asgn for itself
    Then group synchronization succeeds and returns assignment asgn
    When the member heartbeats in its joined generation
    Then the group heartbeat succeeds
    When the member commits offset 42 for topic t partition zero
    Then the group offset commit succeeds
    When the client fetches group g committed offset for topic t partition zero
    Then the fetched committed offset is 42
    When the member leaves group g
    Then the group leave succeeds
