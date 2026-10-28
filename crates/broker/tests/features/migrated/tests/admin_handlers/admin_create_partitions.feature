Feature: tests admin handlers admin create partitions

  Scenario: automatic placement takes a fenced broker only as a last resort
    Then admin handlers admin create partitions: automatic placement takes a fenced broker only as a last resort

  Scenario: create partitions extends topic
    Then admin handlers admin create partitions: create partitions extends topic

  Scenario: create partitions honors explicit assignments
    Then admin handlers admin create partitions: create partitions honors explicit assignments
