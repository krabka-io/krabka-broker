Feature: ListPartitionReassignments exposes ongoing partitions once per requested occurrence
  Scenario Outline: Reassignment listings preserve request order occurrences and ongoing status
    Given topic t has reassigning partitions 0 and 1 and idle partition 2
    And the reassignment request filters <filter>
    When the controller lists the requested reassignments
    Then the complete reassignment listing contains ordered topic partitions <expected>
    Examples:
      | filter     | expected |
      | all topics | t:0,1    |
      | t:none     | none     |
      | t:1        | t:1      |
      | t:1,1      | t:1,1    |
      | t:1,0      | t:1,0    |
      | t:2        | none     |
      | u:0        | none     |
      | t:0;t:1    | t:0;t:1  |
