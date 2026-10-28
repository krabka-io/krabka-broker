Feature: tests soak cluster

  Scenario Outline: tests: a diskless quorum node dials its peers authenticated
    Then soak cluster tests: a diskless quorum node dials its peers authenticated for <example>

    Examples:
      | example |
      | first   |
      | third   |

  Scenario Outline: tests: the token names the principal a wal leader maps to the node
    Then soak cluster tests: the token names the principal a wal leader maps to the node for <example>

    Examples:
      | example |
      | first   |
      | third   |

  Scenario Outline: tests: the host gateway name must reach loopback
    Then soak cluster tests: the host gateway name must reach loopback for <example>

    Examples:
      | example               |
      | loopback              |
      | loopback among others |
      | unresolved            |
      | elsewhere             |
