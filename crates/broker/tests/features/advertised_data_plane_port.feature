Feature: Data plane endpoint discovery
  Scenario: An OS assigned port is advertised in metadata
    Given a standalone broker configured with port zero
    When a client requests cluster metadata
    Then metadata advertises the broker's actual bound port
