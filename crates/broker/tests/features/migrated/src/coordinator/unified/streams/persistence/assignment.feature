Feature: src coordinator unified streams persistence assignment

  @target_assignment_member
  Scenario: Persist a Streams target assignment contains active task 1 in subtopology 0 and no standby or warmup tasks
    Given a Streams target assignment contains active task 1 in subtopology 0 and no standby or warmup tasks
    When the coordinator persists the Streams target member assignment in the compacted group log
    Then the persisted Streams target member assignment matches Kafka schema bytes including compact lengths and tagged trailers
  @current_member_assignment
  Scenario: Persist a Streams current assignment at epochs 5 and 4 has unrevoked tasks and six empty task lists
    Given a Streams current assignment at epochs 5 and 4 has unrevoked tasks and six empty task lists
    When the coordinator persists the Streams current member assignment in the compacted group log
    Then the persisted Streams current member assignment matches Kafka schema bytes including compact lengths and tagged trailers
  @current_member_assignment_epochs
  Scenario: Persist Streams active tasks 1 and 2 have epochs 4 and 5 while active task 3 awaits revocation at epoch 3
    Given Streams active tasks 1 and 2 have epochs 4 and 5 while active task 3 awaits revocation at epoch 3
    When the coordinator persists and reads this current_member_assignment_epochs record
    Then the current_member_assignment_epochs bytes match Kafka exactly and decoding recovers all original record fields
  @member-state
  Scenario Outline: Logical Streams states preserve Kafka discriminants
    Given a Streams member is <state>
    When the coordinator converts the Streams state to Kafka wire state and back
    Then the Streams state retains its identity and Kafka discriminant is <code>
    Examples:
      | state | code |
      | stable | 1 |
      | unrevoked | 2 |
      | unreleased | 3 |
  @member-state
  Scenario: Zero is not a Kafka Streams member state
    Given the stored Streams member state discriminant is 0
    When the coordinator decodes the stored Streams member state
    Then the invalid Streams state discriminant is rejected
