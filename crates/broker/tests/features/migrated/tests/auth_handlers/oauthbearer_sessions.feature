Feature: Bearer token expiry and in-band reauthentication on a live connection

  Scenario: oauthbearer session lifetime ms set from token exp
    Given an uncapped OAUTHBEARER listener with zero clock skew
    And a token for alice expires in 600 seconds
    When the client authenticates using that bearer token
    Then the bearer lifetime is at least 590000 and below 605000 milliseconds

  Scenario: oauthbearer session capped by connections max reauth
    Given an OAUTHBEARER listener capped at 300 milliseconds with zero clock skew
    And a token for alice expires in 600 seconds
    When the client authenticates using that bearer token
    Then the capped bearer lifetime is between 200 and 300 milliseconds
    When 500 milliseconds elapse before the bearer client requests Metadata
    And the bearer client reads the connection
    Then the expired or refused bearer connection reaches EOF

  Scenario: oauthbearer session expires closes connection
    Given an uncapped OAUTHBEARER listener with zero clock skew
    And alice is bearer-authenticated with a token expiring in 4 seconds
    When the current bearer token's expiration passes
    And the expired bearer client requests Metadata
    And the bearer client reads the connection
    Then the expired or refused bearer connection reaches EOF

  Scenario: oauthbearer in band reauth with fresh token resets timer
    Given an uncapped OAUTHBEARER listener with zero clock skew
    And alice is bearer-authenticated with a token expiring in 4 seconds
    When the current bearer token's expiration passes
    Given a token for alice expires in 600 seconds
    When the same connection reauthenticates with the new bearer token
    Then in-band bearer reauthentication succeeds past the old token's expiration
    When the authenticated connection requests Metadata
    Then the live session's Metadata response names at least one broker

  Scenario: oauthbearer in band reauth with different principal closes
    Given an uncapped OAUTHBEARER listener with zero clock skew
    And alice is bearer-authenticated with a token expiring in 600 seconds
    And a token for bob expires in 600 seconds
    When the same connection reauthenticates with the new bearer token
    Then changing the bearer principal is refused with SASL_AUTHENTICATION_FAILED
    When the bearer client reads the connection
    Then the expired or refused bearer connection reaches EOF

  Scenario: oauthbearer in band reauth with different mechanism closes
    Given the listener enables OAUTHBEARER and SCRAM-SHA-512 with zero clock skew
    And alice is bearer-authenticated with a token expiring in 600 seconds
    When the authenticated bearer client handshakes as SCRAM-SHA-512
    Then the mechanism-switch handshake succeeds and lists both enabled mechanisms
    When another frame follows the bearer mechanism switch
    Then the frame after the mechanism switch receives no response

  Scenario: plain listener session lifetime ms is zero and no timer
    Given a PLAIN listener has neither idle expiry nor a reauthentication cap
    When alice authenticates with PLAIN and reads the session lifetime
    Then PLAIN handshake and authentication succeed with session lifetime zero
    When the Tokio clock advances by one hour
    And the authenticated connection requests Metadata
    Then the live session's Metadata response names at least one broker
