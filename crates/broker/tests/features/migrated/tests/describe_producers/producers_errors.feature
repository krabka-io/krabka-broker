Feature: Producer inspection returns a row for each requested partition even on errors
  Scenario: unknown topic returns unknown topic or partition
    Given a standalone broker is ready to inspect producer state
    When the client describes producers for topic ghost partitions 0,1
    Then both unknown-topic partition rows return error 3 and no producers

  Scenario: out of range partition returns unknown topic or partition
    Given a standalone broker is ready to inspect producer state
    And producer topic small exists with one partition
    When the client describes producers for topic small partitions 0,5
    Then partition 0 succeeds while nonexistent partition 5 returns error 3

  Scenario: metadata known partition not hosted locally returns not leader
    Given a two broker cluster knows remote partition 0 but the connected broker does not host it
    When the client describes producers for topic remote partitions 0
    Then the non-hosted partition row has index 0 error 6 and no producers
