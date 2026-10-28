Feature: Barrier markers use Kafka control record keys
  Consumers recognize barrier records as control records and can skip them.

  Scenario: Encode a barrier beyond Kafka's assigned control type range
    Given barrier group orders-cut at epoch 7 was triggered at 1724500000000
    When the barrier control batch is built at offset 42 in leader epoch 3
    Then the control record key contains Kafka version 0 and barrier type 1000
