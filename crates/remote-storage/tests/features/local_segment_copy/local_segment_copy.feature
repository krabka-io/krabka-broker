Feature: Local tiered segment copies
  Scenario: A tiered copy preserves every artifact at its Kafka-compatible path
    Given a local orders segment with every Kafka index artifact
    When the segment is copied into local tiered storage
    Then the Kafka tiered partition contains exactly these segment artifacts
      | suffix                   | contents   |
      | .log                     | 0123456789 |
      | .index                   | OFFSET-IDX |
      | .timeindex               | TIME-IDX   |
      | .snapshot                | SNAP       |
      | .leader_epoch_checkpoint | EPOCH-BYTES |
      | .txnindex                | TXN-IDX    |
