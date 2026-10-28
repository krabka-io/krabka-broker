Feature: src coordinator unified share persistence partition metadata

  @state_partition_metadata
  Scenario: Persist a Share group has initialized orders partition 0 and is deleting carts
    Given a Share group has initialized orders partition 0 and is deleting carts
    When the coordinator persists and reads this state_partition_metadata record
    Then the state_partition_metadata bytes match Kafka exactly and decoding recovers all original record fields
