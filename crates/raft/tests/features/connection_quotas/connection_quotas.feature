Feature: Connection quotas cap the connections a controller listener accepts

  Rule: max.connections caps every listener but the inter-broker listener

    Scenario: A connection past max.connections is refused
      Given max.connections is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.2 connects on listener PLAINTEXT
      Then the connection is refused by max.connections

    Scenario: The inter-broker listener accepts past max.connections
      Given max.connections is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.2 connects on the inter-broker listener REPLICATION
      Then the connection is accepted

  Rule: A listener's max.connections counts only that listener's connections

    Scenario Outline: A connection past the listener cap is refused
      Given the listener max.connections is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.2 connects on listener <listener>
      Then the connection is refused by the listener max.connections

      Examples:
        | case                                  | listener  |
        | the same listener                     | PLAINTEXT |
        | the same listener named in lower case | plaintext |

    Scenario: Another listener's connections leave room under its cap
      Given the listener max.connections is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.2 connects on listener EXTERNAL
      Then the connection is accepted

  Rule: max.connections.per.ip caps the connections from one address

    Scenario: A second connection from the same address is refused
      Given max.connections.per.ip is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.1 connects on listener PLAINTEXT
      Then the connection is refused by max.connections.per.ip

    Scenario: Another address has its own allowance
      Given max.connections.per.ip is 1
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.2 connects on listener PLAINTEXT
      Then the connection is accepted

  Rule: max.connections.per.ip.overrides replaces the per-IP cap of its address

    Scenario: An override raises the cap of its address
      Given max.connections.per.ip is 1
      And max.connections.per.ip.overrides gives 10.0.0.1 a cap of 2
      And a client at 10.0.0.1 holds a connection on listener PLAINTEXT
      When a client at 10.0.0.1 connects on listener PLAINTEXT
      Then the connection is accepted

    Scenario: An override of zero refuses its address
      Given max.connections.per.ip.overrides gives 10.0.0.2 a cap of 0
      When a client at 10.0.0.2 connects on listener PLAINTEXT
      Then the connection is refused by max.connections.per.ip
