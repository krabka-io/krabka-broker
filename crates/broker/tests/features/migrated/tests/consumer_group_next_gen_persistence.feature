Feature: Replay next-generation consumer membership and subscriptions

  Scenario: replay preserves group epoch and members
    Given a disk-backed consumer broker with topic tp and 2 partitions
    When a member joins group gp with named topics ["tp"] and regex null
    Then the persisted consumer heartbeat returns Kafka code 0
    When the broker shuts down after its one member is visible
    And the restarted broker receives the saved member heartbeat at epoch acknowledged with rebalance timeout 60000 and named topics ["tp"]
    Then the persisted consumer heartbeat returns Kafka code 0
    And the restarted consumer broker is shut down

  Scenario: next gen state cleared after leave then restart
    Given a disk-backed consumer broker with topic tp2 and 1 partitions
    When a member joins group gpx with named topics ["tp2"] and regex null
    Then the persisted consumer heartbeat returns Kafka code 0
    When the broker waits 75 milliseconds for the live group to become empty
    Then the group-empty waiter times out while the member is live
    When the member leaves at epoch minus one and the broker shuts down after its group is empty
    And the restarted broker receives the saved member heartbeat at epoch 5 with rebalance timeout -1 and named topics ["tp2"]
    Then the persisted consumer heartbeat returns Kafka code 25
    And the restarted consumer broker is shut down

  Scenario: replay keeps the topics a regex resolved to and finds new ones
    Given a disk-backed consumer broker with topic orders-eu and 2 partitions
    When a member joins group gre with named topics null and regex "orders-.*"
    Then the persisted consumer heartbeat returns Kafka code 0
    When the member heartbeats at its acknowledged epoch without repeating the regex
    Then the persisted consumer heartbeat returns Kafka code 0
    And the heartbeat assignment includes exactly 1 topics
    When the broker shuts down after its one member is visible
    And the replayed coordinator creates topic orders-us with 2 partitions and polls the saved member up to 200 times every 50 milliseconds
    Then all replayed pattern heartbeats return Kafka code 0 and the member holds 2 topics
    And the restarted consumer broker is shut down
