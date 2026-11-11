Feature: Barrier markers use Kafka control batch conventions

  Rule: A barrier marker is a control batch that no producer or transaction owns

    Scenario: A barrier marker carries no producer identity
      Given barrier group orders-cut at epoch 7 was triggered at 1724500000000
      When the barrier control batch is built at offset 42 in leader epoch 3
      Then the batch is a non-transactional control batch with no producer identity

  Rule: A barrier marker is one record at the offset, leader epoch and trigger time it is built with

    Scenario: A barrier marker takes its position and its timestamp from the trigger
      Given barrier group orders-cut at epoch 7 was triggered at 1724500000000
      When the barrier control batch is built at offset 42 in leader epoch 3
      Then the batch holds one record at offset 42 in leader epoch 3 stamped at 1724500000000

  Rule: The control record key is Kafka key version 0 with a type beyond Kafka's assigned range

    Scenario: Encode a barrier beyond Kafka's assigned control type range
      Given barrier group orders-cut at epoch 7 was triggered at 1724500000000
      When the barrier control batch is built at offset 42 in leader epoch 3
      Then the control record key is Kafka key version 0 with barrier type 1000
