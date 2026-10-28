Feature: Rejecting non-reauthentication requests on expired SASL sessions

  Scenario: plain expired session closes on a bare sasl authenticate
    Given alice is already authenticated using PLAIN with a 300 millisecond window
    When 500 milliseconds pass before a bare SaslAuthenticate for alice
    Then the expired bare SaslAuthenticate receives no response
    When the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes

  Scenario: plain expired session closes on a handshake inside the reauth interval
    Given alice is already authenticated using PLAIN with a 300 millisecond window
    When 1050 milliseconds pass before alice reauthenticates
    Then authentication succeeds
    When 500 milliseconds pass before another SaslHandshake
    Then the expired SaslHandshake receives no response
    When the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes
