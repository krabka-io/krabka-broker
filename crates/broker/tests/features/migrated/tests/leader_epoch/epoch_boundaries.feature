Feature: Leader epochs persist append boundaries and fence stale or future replica fetches
  Scenario: Successive epochs persist Kafka compatible checkpoint rows
    Given a single broker hosts topic ckpt at leader epoch zero
    When a producer appends record v0 under the current leader epoch
    And the controller advances the topic leader epoch to 1
    And a producer appends record v1 under the current leader epoch
    And the leader epoch checkpoint is read from the partition directory
    Then the checkpoint version is 0 its row count is 2 and its epoch offsets contain
      | epoch | offset |
      | 0 | 0 |
      | 1 | 1 |

  Scenario: A stale fetch epoch is fenced by the current leader
    Given a single broker hosts topic fence at leader epoch zero
    And record v0 is already appended under leader epoch zero
    When the controller advances the topic leader epoch to 5
    And replica ninety nine fetches with current leader epoch 2
    Then the fetch partition returns Kafka epoch error code 74

  Scenario: A future fetch epoch reports unknown leader epoch
    Given a single broker hosts topic unknown at leader epoch zero
    When replica ninety nine fetches with current leader epoch 5
    Then the fetch partition returns Kafka epoch error code 75

  Scenario: A follower fetch beyond its claimed epoch receives the actual divergence boundary
    Given a single broker hosts topic diverge at leader epoch zero
    When a producer appends record e0-a under the current leader epoch
    And a producer appends record e0-b under the current leader epoch
    And the controller starts leader epoch 1 at offset 2 with assigned follower 7
    And a producer appends record e1-a under the current leader epoch
    And a producer appends record e1-b under the current leader epoch
    Then the leader's local log ends at offset 4
    When assigned follower 7 fetches offset 4 claiming last fetched epoch 0
    Then the successful fetch reports divergence epoch 0 ending at offset 2 without records
