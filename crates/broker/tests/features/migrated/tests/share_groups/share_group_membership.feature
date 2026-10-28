Feature: Share group members receive assignments, leave, and recover durable membership

  Scenario: single member join assignment
    Given a share coordinator with topic t1 containing 4 partitions
    When a new member joins share group g1 at epoch zero
    Then the first share join succeeds at epoch 2 with the requested identity and an empty assignment
    When the first member heartbeats until initialized share partitions are assigned
    Then the single share member owns all four partitions at epoch 3

  Scenario: two members then describe
    Given a share coordinator with topic t2 containing 4 partitions
    When a new member joins share group g1 at epoch zero
    And a new member joins share group g1 at epoch zero
    Then both share joins succeeded with distinct member identities
    When the first member heartbeats at its returned epoch with the same subscription
    Then the share membership heartbeat succeeds
    When an administrator describes share group g1
    Then the described share group contains 2 members and has a positive group epoch

  Scenario: member leave epoch minus one
    Given a share coordinator with topic t3 containing 2 partitions
    And a share member has completed joining g1 for the topic
    When the joined share member leaves at epoch minus one
    Then the share membership heartbeat succeeds
    When an administrator describes share group g1
    Then the retained share group contains no members

  Scenario: state survives restart
    Given a share coordinator with topic t4 containing 2 partitions
    And a share member has completed joining g1 for the topic
    When the share coordinator restarts on its durable data directory
    And an administrator describes share group g1
    Then the recovered share group contains the original member identity and a positive epoch
