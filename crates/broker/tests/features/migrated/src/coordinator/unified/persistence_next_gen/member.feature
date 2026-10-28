Feature: src coordinator unified persistence next gen member

  @member_metadata
  Scenario: Persist a consumer member i1 at client c1 subscribes to a and b with the uniform assignor and timeout 60000
    Given a consumer member i1 at client c1 subscribes to a and b with the uniform assignor and timeout 60000
    When the coordinator persists the consumer member metadata in the compacted group log
    Then the persisted consumer member metadata matches Kafka schema bytes including compact lengths and tagged trailers
  @classic_sub_state
  Scenario: Persist a consumer member c at host h carries classic range protocol metadata m with session timeout 2
    Given a consumer member c at host h carries classic range protocol metadata m with session timeout 2
    When the coordinator persists the consumer member metadata in the compacted group log
    Then the persisted consumer member metadata matches Kafka schema bytes including compact lengths and tagged trailers
