Feature: OAUTHBEARER token validation and the RFC 7628 failure handshake

  Scenario: sasl oauthbearer happy path
    Given an OAUTHBEARER listener accepting unsecured JWS tokens
    And an unsecured svc-account token expiring one hour from now
    And the client has selected OAUTHBEARER on a new connection
    When the bearer token is sent as the initial SASL response
    Then the token is accepted in one round without challenge bytes
    When the bearer-authenticated connection requests Metadata
    Then Metadata succeeds after admitting the bearer principal

  Scenario: sasl oauthbearer invalid token two round failure
    Given an unsecured OAUTHBEARER listener allowing zero clock skew
    And an unsecured admin token that expired one hour ago
    And the client has selected OAUTHBEARER on a new connection
    When the bearer token is sent as the initial SASL response
    Then round 1 keeps the connection open and returns only the invalid_token JSON challenge
    When the client sends the RFC 7628 dummy byte 1
    Then round 2 returns SASL_AUTHENTICATION_FAILED

  Scenario: sasl oauthbearer signed token happy path
    Given an OAUTHBEARER listener trusting ES256 key A as kid k1 in an in-memory JWKS
    And a svc-account ES256 token signed by key A and expiring one hour from now
    And the client has selected OAUTHBEARER on a new connection
    When the bearer token is sent as the initial SASL response
    Then the token is accepted in one round without challenge bytes
    When the bearer-authenticated connection requests Metadata
    Then Metadata succeeds after admitting the bearer principal

  Scenario: sasl oauthbearer signed token wrong key two round failure
    Given an OAUTHBEARER listener trusting ES256 key A as kid k1 in an in-memory JWKS
    And an admin ES256 token signed by different key B under the same kid k1
    And the client has selected OAUTHBEARER on a new connection
    When the bearer token is sent as the initial SASL response
    Then round 1 keeps the connection open and returns only the invalid_token JSON challenge
    When the client sends the RFC 7628 dummy byte 1
    Then round 2 returns SASL_AUTHENTICATION_FAILED
