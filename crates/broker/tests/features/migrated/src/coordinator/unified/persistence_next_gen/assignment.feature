Feature: src coordinator unified persistence next gen assignment

  @target_assignment_member
  Scenario: Persist TargetAssignmentMemberValue using Kafka compacted-log schema
    Given a consumer target assignment assigns partitions 0 and 1 of the all-one topic UUID
    When the coordinator encodes this TargetAssignmentMemberValue record for the compacted group log
    Then the TargetAssignmentMemberValue record matches Kafka schema bytes including compact lengths and tagged trailers
  @current_member_assignment
  Scenario: Persist consumer member epochs 5 and 4 hold partitions 7 and 8 at assignment epochs 3 and 5 while partition 1 awaits revocation
    Given consumer member epochs 5 and 4 hold partitions 7 and 8 at assignment epochs 3 and 5 while partition 1 awaits revocation
    When the coordinator persists and reads this current_member_assignment record
    Then the current_member_assignment bytes match Kafka exactly and decoding recovers all original record fields
  @member-state
  Scenario Outline: Kafka consumer member states decode and retain their discriminants
    Given a consumer member state is stored with discriminant <code>
    When the coordinator decodes the Kafka consumer member state
    Then the consumer state is <state> and its wire discriminant is preserved
    Examples:
      | code | state |
      | 0 | stable |
      | 1 | unrevoked |
      | 2 | unreleased |
      | 3 | rejected |
