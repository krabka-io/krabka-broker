Feature: SyncGroup checks mandatory protocol fields and authorization in Kafka order
  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=sync-group-unit (1)
    Given the SyncGroup caller is authorized false
    And a SyncGroup version 5 request names group sync-group-unit protocol type null and name range
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 23

  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=5, group=sync-group-unit (2)
    Given the SyncGroup caller is authorized true
    And a SyncGroup version 5 request names group sync-group-unit protocol type consumer and name null
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 23

  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=4, group=sync-group-unit (3)
    Given the SyncGroup caller is authorized true
    And a SyncGroup version 4 request names group sync-group-unit protocol type null and name null
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 25

  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=sync-group-unit (4)
    Given the SyncGroup caller is authorized false
    And a SyncGroup version 5 request names group sync-group-unit protocol type consumer and name range
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 30

  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=5, group=empty (5)
    Given the SyncGroup caller is authorized true
    And a SyncGroup version 5 request names group empty protocol type consumer and name range
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 24

  Scenario: Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=empty (6)
    Given the SyncGroup caller is authorized false
    And a SyncGroup version 5 request names group empty protocol type consumer and name range
    When alice synchronizes the requested group
    Then the complete SyncGroup response contains only error 30

