Feature: Growing partitions with valid and available replica assignments

  Scenario: automatic placement takes a fenced broker only as a last resort
    Given three brokers are registered and one non-controller peer is fenced
    When the admin creates topic t-usable-brokers with replication factor 2
    Then the placement topic creation succeeds and partition 0 exists
    When the admin grows the topic to 3 partitions with automatic placement
    Then CreatePartitions succeeds
    And none of the 3 partitions assigns the fenced broker as a replica or ISR member
    When the admin creates topic t-last-resort with replication factor 3
    Then the placement topic creation succeeds and partition 0 exists
    And the fenced replica is last and remains outside the ISR and leadership
    When the admin creates topic t-too-many-replicas with replication factor 4
    Then CreateTopics rejects the replication factor with INVALID_REPLICATION_FACTOR

  Scenario: create partitions extends topic
    Given an admin client connected to one standalone broker
    And topic t-cp starts with one partition
    When the admin grows the topic to 3 partitions with automatic placement
    Then CreatePartitions succeeds
    And all 3 partitions materialize locally

  Scenario: create partitions honors explicit assignments
    Given an admin client connected to one standalone broker
    And topic t-cpa starts with one partition
    When the admin grows the topic to 2 partitions with 1 assignments to broker 1
    Then CreatePartitions succeeds
    And all 2 partitions materialize locally
    When the admin grows the topic to 3 partitions with 2 assignments to broker 1
    Then CreatePartitions rejects the assignment count with INVALID_REPLICA_ASSIGNMENT
    And partition 2 has not been created
