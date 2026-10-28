Feature: Cold coordinator topics retain configured replication through broker loss
  Scenario: a cold cluster replicates the offsets topic on every broker
    Given a cold three broker cluster uses replication factor 3 for coordinator topics
    Then the offsets topic is absent before any group coordinator lookup
    When a client first looks up the coordinator for group g
    Then the initial lookup reports coordinator not available error 15
    When the client retries group g until its coordinator is available
    Then all three offsets partitions have replicas on brokers 1 2 and 3
    When the broker coordinating group g shuts down
    And a survivor looks up group g until a different coordinator is available
    Then the surviving coordinator differs from the stopped broker

  Scenario: a replication factor above the broker count leaves the coordinator unavailable
    Given a cold three broker cluster requires transaction state replication factor 4
    When the client retries group g until its coordinator is available
    Then the group coordinator belongs to the running cluster
    When the client performs ten transaction coordinator lookups for t
    Then all ten lookups report error 15 and no broker image contains a transaction state topic

  Scenario: a cleanly stopped broker still counts toward the replication factor
    Given a cold three broker cluster uses three share-state partitions
    And a non-quorum-leader broker is selected after led is created with three partitions and replication factor 3
    When the selected broker completes a controlled shutdown within 30 seconds
    Then the stopped broker stays registered fenced and in controlled shutdown on every survivor
    When a survivor looks up share partition 0 with topic UUID bytes all 7
    Then all three share-state partitions retain replicas 1 2 3 with only the survivors in ISR
