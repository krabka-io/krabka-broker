Feature: Only a partition leader accepts a Produce and mutates its log

  Scenario: produce to non leader is rejected
    Given three registered brokers host gate-rf3 with one RF3 partition and gate-rf1 with six RF1 partitions, with every leader elected
    When a one-record rf3-to-follower Produce is sent directly to a materialized RF3 follower and its log end is measured before and after
    Then the RF3 follower returns NOT_LEADER_OR_FOLLOWER 6 with the real leader id and its complete PLAINTEXT endpoint, and its local log does not grow
    When node 1 receives rf1-to-nonreplica for an RF1 partition led elsewhere and hosted on no local replica
    Then the non-replica returns NOT_LEADER_OR_FOLLOWER 6 and hints the actual RF1 leader
    When the RF3 leader installs its leadership and receives rf3-to-leader with its log end measured before and after
    Then the RF3 leader returns NONE 0 and appends exactly one record
    When the RF1 leader installs its leadership and receives rf1-to-leader
    Then the RF1 leader returns NONE 0 and durably holds at least one record
