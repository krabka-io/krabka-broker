Feature: Local tiered segment copies
  Scenario Outline: A tiered copy exposes every artifact at its Kafka-compatible path
    Given a local orders segment with every Kafka index artifact
    When the segment is copied into local tiered storage
    Then the Kafka tiered partition contains segment artifact <suffix>

    Examples:
      | case                   | suffix                   |
      | the record log         | .log                     |
      | the offset index       | .index                   |
      | the time index         | .timeindex               |
      | the producer snapshot  | .snapshot                |
      | the leader epoch cache | .leader_epoch_checkpoint |
      | the transaction index  | .txnindex                |
