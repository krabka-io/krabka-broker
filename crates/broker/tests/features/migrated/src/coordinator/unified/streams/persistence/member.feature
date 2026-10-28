Feature: src coordinator unified streams persistence member

  @member_metadata
  Scenario: Persist a Streams member c at host h with process p endpoint e:8080 tag k=v timeout 1 and topology epoch 2
    Given a Streams member c at host h with process p endpoint e:8080 tag k=v timeout 1 and topology epoch 2
    When the coordinator persists the Streams member metadata in the compacted group log
    Then the persisted Streams member metadata matches Kafka schema bytes including compact lengths and tagged trailers
