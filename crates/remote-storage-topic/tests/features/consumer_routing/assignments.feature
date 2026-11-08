Feature: Metadata consumer partition assignments
  Scenario: Assignments track replacement and removal of partition tasks
    Given a metadata consumer has fetch wait 750ms size 2MiB and retry delay 300ms
    When partition 2 is spawned at 7 then 9 and partition 0 is added before removing partition 2
    Then only partition 0 remains assigned and the configured fetch policy is retained
