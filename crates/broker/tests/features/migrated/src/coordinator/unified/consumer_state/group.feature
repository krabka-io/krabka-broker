Feature: Consumer group state epochs and offset commit fencing
  Scenario Outline: Group state reflects membership target epoch and reconciliation
    Given a consumer group at epoch <epoch> target epoch <target> has member epoch <member_epoch> in state <member_state>
    When the coordinator describes the consumer group state
    Then the consumer group state is <state>
    Examples:
      | epoch | target | member_epoch | member_state | state |
      | 3 | 3 | absent | stable | Empty |
      | 4 | 3 | absent | stable | Empty |
      | 4 | 3 | 3 | stable | Assigning |
      | 3 | 3 | 2 | stable | Reconciling |
      | 3 | 3 | 3 | unreleased | Reconciling |
      | 3 | 3 | 3 | stable | Stable |
  @commit-coverage-reference
  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (1: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 5 using offset-v9 for partitions 0,1,3
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (2: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 6 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (3: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (4: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions 2
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (5: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 3 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (6: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions 1
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (7: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions 3
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (8: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions 0,1
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (9: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using offset-v9 for partitions none
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (10: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 5 using offset-v8 for partitions 0
    Then the offset commit returns Kafka error code 35

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (11: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 5 using offset-v8 for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (12: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 6 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 22

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (13: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 4 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (14: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 4 using offset-v9 for partitions 1
    Then the offset commit returns Kafka error code 22

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (15: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 4 using offset-v9 for partitions 3
    Then the offset commit returns Kafka error code 22

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (16: member empty, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member empty instance absent commits at epoch -1 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 25

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (17: member ghost, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member ghost instance absent commits at epoch 5 using offset-v9 for partitions 0
    Then the offset commit returns Kafka error code 25

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (18: member empty, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member empty instance absent commits at epoch -1 using transactional for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (19: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 5 using transactional for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (20: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using transactional for partitions 0
    Then the offset commit returns Kafka error code 0

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (21: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 4 using transactional for partitions 1
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (22: member native, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member native instance absent commits at epoch 6 using transactional for partitions 0
    Then the offset commit returns Kafka error code 113

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (23: member classic, instance absent)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member classic instance absent commits at epoch 4 using transactional for partitions 3
    Then the offset commit returns Kafka error code 22

  Scenario: Offset commits enforce member generation and per-partition assignment epoch fences (24: member empty, instance i1)
    Given native and classic members at epoch 5 own partition 0 since epoch 3 partition 1 since epoch 5 and revoke partition 2 since epoch 2
    When member empty instance i1 commits at epoch -1 using transactional for partitions 0
    Then the offset commit returns Kafka error code 25

  Scenario: Assignment epochs survive retained and revoked partitions and reset on static leave
    Given consumer member m1 subscribes to topic t before any assignment
    When target epoch 1 assigns partitions 0,1 and m1 reports owned partitions none joining true
    Then the partition assignment epochs are 0:1,1:1
    When target epoch 2 assigns partitions 0 and m1 reports owned partitions 0,1 joining false
    Then the partition assignment epochs are 0:1,1:1
    When m1 reports revoking partition 1 while retaining partition 0
    Then the partition assignment epochs are 0:1
    When target epoch 3 assigns partitions 0,2 and m1 reports owned partitions 0 joining false
    Then the partition assignment epochs are 0:1,2:3
    When m1 temporarily leaves as a static member
    Then the partition assignment epochs are 0:0,2:0
