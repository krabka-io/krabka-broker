Feature: Kafka share-state coordinator record layouts
  Snapshot and update records retain Kafka's version-zero field order, compact
  batch arrays, tag trailers, and optional delivery-complete-count tag.

  Scenario: An initial empty snapshot omits the default delivery-complete-count tag
    Given a share snapshot has snapshot epoch 0, state epoch 1, leader epoch 0, and start offset -1
    And snapshot creation time is 1000, write time is 1000, and delivery-complete count is -1
    And the persisted share-state batches are:
      | first | last | delivery | attempts |
    When the share snapshot is encoded and decoded as a coordinator record
    Then the version-zero share record bytes are 0000 00000000 00000001 00000000 ffffffffffffffff 00000000000003e8 00000000000003e8 01 00
    And decoding preserves every field of the share snapshot

  Scenario: A populated snapshot retains batches, timestamps, and the delivery-complete tag
    Given a share snapshot has snapshot epoch 3, state epoch 2, leader epoch 4, and start offset 10
    And snapshot creation time is 1000, write time is 2000, and delivery-complete count is 5
    And the persisted share-state batches are:
      | first | last | delivery     | attempts |
      | 10    | 19   | available    | 1        |
      | 20    | 29   | acknowledged | 3        |
    When the share snapshot is encoded and decoded as a coordinator record
    Then the version-zero share record bytes are 0000 00000003 00000002 00000004 000000000000000a 00000000000003e8 00000000000007d0 03 000000000000000a 0000000000000013 00 0001 00 0000000000000014 000000000000001d 02 0003 00 01 00 04 00000005
    And decoding preserves every field of the share snapshot

  Scenario: A batch update omits the default delivery-complete-count tag
    Given a share update has snapshot epoch 3, leader epoch 4, start offset 12, and delivery-complete count -1
    And the persisted share-state batches are:
      | first | last | delivery  | attempts |
      | 12    | 15   | available | 1        |
    When the share update is encoded and decoded as a coordinator record
    Then the version-zero share record bytes are 0000 00000003 00000004 000000000000000c 02 000000000000000c 000000000000000f 00 0001 00 00
    And decoding preserves every field of the share update

  Scenario: An empty-batch update still retains a supplied delivery-complete-count tag
    Given a share update has snapshot epoch 3, leader epoch 4, start offset 12, and delivery-complete count 7
    And the persisted share-state batches are:
      | first | last | delivery | attempts |
    When the share update is encoded and decoded as a coordinator record
    Then the version-zero share record bytes are 0000 00000003 00000004 000000000000000c 01 01 00 04 00000007
    And decoding preserves every field of the share update
