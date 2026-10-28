Feature: Kafka initial SASL handshake and KIP-368 reauthentication
  Mechanism admission, one-second reauthentication throttling, preserved sessions,
  deferred mechanism-change failures, and close decisions follow Kafka together.

  Background:
    Given the SASL listener enables OAUTHBEARER and SCRAM-SHA-512 at time 10000000

  Scenario: An enabled initial mechanism starts negotiation
    Given the SASL connection has not authenticated
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism OAUTHBEARER
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 0 | ["OAUTHBEARER","SCRAM-SHA-512"] | false | Negotiating:OAUTHBEARER | never |

  Scenario: A disabled initial mechanism lists enabled choices and closes
    Given the SASL connection has not authenticated
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism PLAIN
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 33 | ["OAUTHBEARER","SCRAM-SHA-512"] | true | Anonymous | never |

  Scenario: An unknown initial mechanism lists enabled choices and closes
    Given the SASL connection has not authenticated
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism NOPE
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 33 | ["OAUTHBEARER","SCRAM-SHA-512"] | true | Anonymous | never |

  Scenario: An unbounded session cannot begin reauthentication
    Given the prior OAUTHBEARER session has no expiry
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism OAUTHBEARER
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 34 | [] | false | Authenticated | never |

  Scenario: Reauthentication inside one second keeps the existing session and clock
    Given the prior OAUTHBEARER session expires at 10005000
    And the last SASL reauthentication started at 9999001
    When the client handshakes with SASL mechanism OAUTHBEARER
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 34 | [] | false | Authenticated | 9999001 |

  Scenario: The same mechanism may reauthenticate at the one-second boundary
    Given the prior OAUTHBEARER session expires at 10005000
    And the last SASL reauthentication started at 9999000
    When the client handshakes with SASL mechanism OAUTHBEARER
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 0 | ["OAUTHBEARER","SCRAM-SHA-512"] | false | Reauthenticating:OAUTHBEARER | 10000000 |

  Scenario: Switching to another enabled mechanism defers failure to the next frame
    Given the prior OAUTHBEARER session expires at 10005000
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism SCRAM-SHA-512
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 0 | ["OAUTHBEARER","SCRAM-SHA-512"] | false | ReauthBadMechanism | 10000000 |

  Scenario: An unsupported reauthentication mechanism closes without replacing the session
    Given the prior OAUTHBEARER session expires at 10005000
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism PLAIN
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 33 | ["OAUTHBEARER","SCRAM-SHA-512"] | true | Authenticated | 10000000 |

  Scenario: A second handshake during negotiation is illegal and closes
    Given an OAUTHBEARER exchange is already negotiating
    And the last SASL reauthentication started at never
    When the client handshakes with SASL mechanism OAUTHBEARER
    Then the SASL reply, connection state, and reauthentication clock are:
      | code | mechanisms | close | state | last_start |
      | 34 | [] | true | Negotiating:OAUTHBEARER | never |
