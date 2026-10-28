Feature: src coordinator unified share persistence member

  @member_metadata
  Scenario: Persist a Share member c at host h in rack r subscribes to topic a
    Given a Share member c at host h in rack r subscribes to topic a
    When the coordinator persists the Share member metadata in the compacted group log
    Then the persisted Share member metadata matches Kafka schema bytes including compact lengths and tagged trailers
