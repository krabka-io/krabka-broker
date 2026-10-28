Feature: Local tiered segment copies
  Scenario Outline: A tiered copy exposes every artifact at its Kafka-compatible path
    Given a local orders segment with every Kafka index artifact
    When the segment is copied into local tiered storage
    Then the Kafka tiered partition contains segment artifact <suffix>

    Examples:
      | suffix                   |
      | .log                     |
      | .index                   |
      | .timeindex               |
      | .snapshot                |
      | .leader_epoch_checkpoint |
      | .txnindex                |
