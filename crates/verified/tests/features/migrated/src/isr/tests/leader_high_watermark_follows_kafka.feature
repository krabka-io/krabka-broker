Feature: Committed high watermarks and controller ISR admission
  Commit cannot regress, cannot advance below minimum ISR, and respects eligible
  caught-up followers. Controller ISR updates reject stale or malformed proposals
  in Kafka response precedence.

  Scenario: The slowest in-sync follower sets the watermark
    Given a leader at high watermark 40 and log end 100 with ISR size 3 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 80 | true | false | false |
      | 60 | true | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 60

  Scenario: A leader alone in the ISR advances to its log end
    Given a leader at high watermark 40 and log end 100 with ISR size 1 and minimum ISR 1
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 10 | false | false | false |
      | 20 | false | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 100

  Scenario: An ISR below its minimum cannot advance commitment
    Given a leader at high watermark 40 and log end 100 with ISR size 1 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 100 | false | true | false |
      | 100 | false | true | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 40

  Scenario: An ISR exactly at its minimum may commit
    Given a leader at high watermark 40 and log end 100 with ISR size 2 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 70 | true | false | false |
      | 10 | false | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 70

  Scenario: A caught-up eligible follower outside ISR constrains commitment
    Given a leader at high watermark 40 and log end 100 with ISR size 2 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 90 | true | false | false |
      | 55 | false | true | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 55

  Scenario: A lagging follower outside ISR does not constrain commitment
    Given a leader at high watermark 40 and log end 100 with ISR size 2 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 90 | true | false | false |
      | 55 | false | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 90

  Scenario: A fenced follower outside ISR does not constrain commitment
    Given a leader at high watermark 40 and log end 100 with ISR size 2 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 90 | true | false | false |
      | 55 | false | true | true |
    When the leader advances its committed high watermark
    Then the committed high watermark is 90

  Scenario: An unfetched ISR member prevents commitment from advancing
    Given a leader at high watermark 40 and log end 100 with ISR size 3 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 90 | true | false | false |
      | -1 | true | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 40

  Scenario: A follower behind the committed frontier cannot regress it
    Given a leader at high watermark 40 and log end 100 with ISR size 3 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 90 | true | false | false |
      | 30 | true | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 40

  Scenario: An ISR member at the leader end permits full commitment
    Given a leader at high watermark 40 and log end 100 with ISR size 2 and minimum ISR 2
    And its assigned followers have this replication state:
      | log_end | in_isr | recently_caught_up | fenced |
      | 100 | true | false | false |
      | 0 | false | false | false |
    When the leader advances its committed high watermark
    Then the committed high watermark is 100

  Scenario: The controller validates ISR proposals in Kafka precedence order (1: boundary current shrink, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 10, leader identity true, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns NONE

  Scenario: The controller validates ISR proposals in Kafka precedence order (2: boundary newer leader epoch, leader_epoch 6)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 6, partition epoch 10, leader identity true, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns NOT_CONTROLLER

  Scenario: The controller validates ISR proposals in Kafka precedence order (3: boundary newer partition epoch, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 11, leader identity true, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns NOT_CONTROLLER

  Scenario: The controller validates ISR proposals in Kafka precedence order (4: boundary new partition outranks fenced leader, leader_epoch 4)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 4, partition epoch 11, leader identity true, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns NOT_CONTROLLER

  Scenario: The controller validates ISR proposals in Kafka precedence order (5: boundary old leader epoch, leader_epoch 4)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 4, partition epoch 9, leader identity false, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns FENCED_LEADER_EPOCH

  Scenario: The controller validates ISR proposals in Kafka precedence order (6: boundary requester is not leader, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 9, leader identity false, ISR valid, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INVALID_REQUEST

  Scenario: The controller validates ISR proposals in Kafka precedence order (7: boundary controller replaced ISR, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 9, leader identity true, ISR invalid, recovery valid true, and replicas eligible false
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INVALID_UPDATE_VERSION

  Scenario: The controller validates ISR proposals in Kafka precedence order (8: boundary unassigned replica, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 10, leader identity true, ISR invalid, recovery valid true, and replicas eligible false
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INVALID_REQUEST

  Scenario: The controller validates ISR proposals in Kafka precedence order (9: boundary leader omitted from ISR, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 10, leader identity true, ISR without-leader, recovery valid true, and replicas eligible true
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INVALID_REQUEST

  Scenario: The controller validates ISR proposals in Kafka precedence order (10: boundary invalid recovery state, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 10, leader identity true, ISR valid, recovery valid false, and replicas eligible false
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INVALID_REQUEST

  Scenario: The controller validates ISR proposals in Kafka precedence order (11: boundary ineligible fenced broker, leader_epoch 5)
    Given the controller holds leader epoch 5 and partition epoch 10
    And an AlterPartition proposal at leader epoch 5, partition epoch 10, leader identity true, ISR valid, recovery valid true, and replicas eligible false
    When the controller validates the AlterPartition proposal
    Then AlterPartition admission returns INELIGIBLE_REPLICA
