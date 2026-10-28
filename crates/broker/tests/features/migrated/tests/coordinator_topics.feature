Feature: tests coordinator topics

  Scenario: a cold cluster replicates the offsets topic on every broker
    Then coordinator topics: a cold cluster replicates the offsets topic on every broker

  Scenario: a replication factor above the broker count leaves the coordinator unavailable
    Then coordinator topics: a replication factor above the broker count leaves the coordinator unavailable

  Scenario: a cleanly stopped broker still counts toward the replication factor
    Then coordinator topics: a cleanly stopped broker still counts toward the replication factor
