Feature: Offline replica projections
  Scenario: offline log dir is reported as an offline replica
    Given six single-replica partitions are assigned across both registered log directories
    When the extra directory goes offline and the client queries Metadata and DescribeTopicPartitions
    Then dead-directory partitions report offline replica 1, leader -1, empty ISR, Metadata error 5, and Describe error 0 with replica 1 eligible and last-known

  Scenario: a follower reports the replicas of a fenced broker offline
    Given a three-replica partition and two brokers that are not controller leader
    When one controller follower crashes and the other serves Metadata until fencing propagates
    Then the non-controller observer reports the dead replica offline with the complete committed partition row
