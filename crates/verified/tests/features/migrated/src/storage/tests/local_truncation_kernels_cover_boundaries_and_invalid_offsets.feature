Feature: Kafka remote storage lifecycle progression
  Lifecycle updates may retry the current state and move forward through Kafka's
  copy and deletion phases. They cannot resurrect objects or skip required phases.

  Scenario: Segment copy and deletion transitions follow Kafka's lifecycle graph
    Given remote segment states copying, copied, deleting and deleted
    When every remote segment state transition is validated
    Then remote segment transitions are permitted by this matrix:
      | source   | copying | copied | deleting | deleted |
      | copying  | allowed    | allowed   | allowed     | forbidden   |
      | copied   | forbidden   | allowed   | allowed     | forbidden   |
      | deleting | forbidden   | forbidden  | allowed     | allowed    |
      | deleted  | forbidden   | forbidden  | forbidden    | allowed    |

  Scenario: Partition deletion begins at marked and cannot move backwards
    Given remote partition deletion states unregistered, marked, started and finished
    When every remote partition deletion transition is validated
    Then remote partition deletion transitions are permitted by this matrix:
      | source       | marked | started | finished |
      | unregistered | allowed   | forbidden   | forbidden    |
      | marked       | allowed   | allowed    | forbidden    |
      | started      | forbidden  | allowed    | allowed     |
      | finished     | forbidden  | forbidden   | allowed     |
