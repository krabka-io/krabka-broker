Feature: src coordinator unified share persistence assignment

  @target_assignment_member
  Scenario: Persist a Share target assignment assigns partition 3 of the all-one topic UUID
    Given a Share target assignment assigns partition 3 of the all-one topic UUID
    When the coordinator persists the Share target member assignment in the compacted group log
    Then the persisted Share target member assignment matches Kafka schema bytes including compact lengths and tagged trailers
  @current_member_assignment
  Scenario: Persist ShareGroupCurrentMemberAssignmentValue using Kafka compacted-log schema
    Given a Share current assignment at epochs 5 and 4 has no assigned partitions
    When the coordinator encodes this ShareGroupCurrentMemberAssignmentValue record for the compacted group log
    Then the ShareGroupCurrentMemberAssignmentValue record matches Kafka schema bytes including compact lengths and tagged trailers
