Feature: Diskless quorum nodes authenticate peer connections

  Scenario Outline: tests: a diskless quorum node dials its peers authenticated
    Given a diskless quorum node <node_id> advertised on port <port>
    When the soak runner applies the generated diskless node configuration
    Then the node authenticates inter-broker traffic with its OAuth identity and advertised port

    Examples:
      | node_id | port  |
      | 1       | 30001 |
      | 3       | 30003 |
