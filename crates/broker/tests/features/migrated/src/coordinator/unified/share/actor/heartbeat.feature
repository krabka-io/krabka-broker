Feature: Share heartbeat assignments initialization leave and member epochs
  Scenario: Uninitialized Share partitions remain unassigned
    Given Share group g subscribes to four-partition topic t with initialized partitions none
    When Share member m1 heartbeats at epoch 0 including topic subscription true
    Then the complete Share response names member m1 at epoch 2 with assignment []
    When Share member m1 heartbeats at epoch 2 including topic subscription false
    Then the complete Share response names member m1 at epoch 2 with assignment omitted
  Scenario: Initialized Share partitions rebalance on joins and omit unchanged assignments
    Given Share group g subscribes to four-partition topic t with initialized partitions all
    When Share member m1 heartbeats at epoch 0 including topic subscription true
    Then the complete Share response names member m1 at epoch 2 with assignment [0,1,2,3]
    When Share member m1 heartbeats at epoch 2 including topic subscription false
    Then the complete Share response names member m1 at epoch 2 with assignment omitted
    When Share member m2 heartbeats at epoch 0 including topic subscription true
    Then the complete Share response names member m2 at epoch 3 with assignment [0,1]
    When Share member m1 heartbeats at epoch 2 including topic subscription false
    Then the complete Share response names member m1 at epoch 3 with assignment [2,3]
    When Share member m1 heartbeats at epoch 3 including topic subscription false
    Then the complete Share response names member m1 at epoch 3 with assignment omitted
    When Share member m1 heartbeats at epoch 3 including topic subscription true
    Then the complete Share response names member m1 at epoch 3 with assignment [2,3]
  Scenario Outline: Known and unknown Share members leave with Kafka durability rules
    Given Share group g subscribes to four-partition topic t with initialized partitions none
    And m1 has joined Share group g at epoch 2
    When Share member <member> leaves with epoch minus one
    Then the Share leave result for <member> appends <batches> batches and leaves group epoch <epoch>
    Examples:
      | member | batches | epoch |
      | m1 | 1 | 3 |
      | m9 | 0 | 2 |
  Scenario Outline: Share member epoch fencing accepts only rejoin previous or current member epochs
    Given Share group g subscribes to four-partition topic t with initialized partitions none
    And m1 joined at epoch 2 then m2 and m3 joined and m1 advanced from previous epoch 2 to current epoch 4
    When Share member <member> heartbeats at epoch <epoch> including topic subscription true
    Then the epoch validation returns the complete Share <outcome> response
    Examples:
      | member | epoch | outcome |
      | m1 | 0 | accepted |
      | m1 | 2 | accepted |
      | m1 | 3 | fenced |
      | m1 | 4 | accepted |
      | m1 | 5 | fenced |
      | m9 | 4 | unknown-member |
