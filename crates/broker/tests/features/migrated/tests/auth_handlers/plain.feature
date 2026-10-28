Feature: PLAIN authentication, connection lifetime and metrics

  Scenario: sasl plain happy path
    Given a PLAIN listener provisioned with alice's configured password
    And alice supplies the configured PLAIN password
    When alice opens a fresh connection and attempts PLAIN authentication followed by Metadata
    Then PLAIN authentication and Metadata complete successfully

  Scenario: sasl plain authentication metrics tick for success and failure
    Given the PLAIN listener exports authentication metrics
    And alice supplies the configured PLAIN password
    When alice opens a fresh connection and attempts PLAIN authentication followed by Metadata
    Then PLAIN authentication and Metadata complete successfully
    Given alice supplies a wrong PLAIN password
    When alice opens a fresh connection and attempts PLAIN authentication followed by Metadata
    Then PLAIN credentials are rejected with SASL_AUTHENTICATION_FAILED
    When the PLAIN authentication metrics are scraped
    Then PLAIN successful and failed authentication counters both equal 1

  Scenario: sasl plain wrong password closes connection
    Given a PLAIN listener provisioned with alice's configured password
    And alice supplies a wrong PLAIN password
    When alice opens a fresh connection and attempts PLAIN authentication followed by Metadata
    Then PLAIN credentials are rejected with SASL_AUTHENTICATION_FAILED
    When the refused PLAIN client reads the connection
    Then the refused PLAIN connection is closed at EOF

  Scenario: plain session capped by connections max reauth then closes
    Given a PLAIN listener with a 300 millisecond reauthentication window
    When alice authenticates on the connection
    Then authentication succeeds
    And the session lifetime is between 200 and 300 milliseconds
    When 500 milliseconds pass before the client requests Metadata
    And the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes

  Scenario: plain in band reauth same principal reopens data plane
    Given alice is already authenticated using PLAIN with a 30000 millisecond window
    When the connection reauthenticates as alice
    Then authentication succeeds
    And the session lifetime is between 29000 and 30000 milliseconds
    When the client requests Metadata on the same connection
    Then Metadata succeeds and names at least one broker

  Scenario: plain in band reauth with different principal closes
    Given alice is already authenticated using PLAIN with a 30000 millisecond window
    When the connection reauthenticates as bob
    Then changing from alice to bob is rejected with SASL_AUTHENTICATION_FAILED
    When the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes

  Scenario: plain authenticate without handshake closes the connection
    Given a PLAIN listener with a 30 second reauthentication cap
    When alice sends SaslAuthenticate without any SaslHandshake
    Then the pre-handshake SaslAuthenticate receives no response

  Scenario: plain reauth after the window elapsed keeps serving round after round
    Given a PLAIN listener with a 300 millisecond reauthentication window
    When alice authenticates on the connection
    Then authentication succeeds
    When alice completes 3 expired reauthentication rounds 1050 milliseconds apart
    Then all 3 rounds succeed with lifetimes between 200 and 300 milliseconds and reachable Metadata
