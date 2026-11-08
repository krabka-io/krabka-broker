Feature: Kafka share-state value wire compatibility
  Scenario: An empty share snapshot preserves its Kafka wire representation
    Given an empty share snapshot at state epoch 1 and timestamp 1000
    And Kafka share-state bytes 0000 00000000 00000001 00000000 ffffffffffffffff 00000000000003e8 00000000000003e8 01 00
    When the share-state value is encoded and its wire bytes are decoded
    Then the share-state codec preserves Kafka scalar array and tag order
    And every incomplete share-state prefix is rejected

  Scenario: A share update preserves its batches and tagged delivery count
    Given a share update at snapshot epoch 3 covering offsets 12 through 15
    And Kafka share-state bytes 0000 00000003 00000004 000000000000000c 02 000000000000000c 000000000000000f 00 0001 00 01 00 04 00000007
    When the share-state value is encoded and its wire bytes are decoded
    Then the share-state codec preserves Kafka scalar array and tag order
    And every incomplete share-state prefix is rejected
