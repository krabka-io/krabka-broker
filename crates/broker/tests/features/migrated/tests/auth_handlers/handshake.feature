Feature: SASL connection admission before and during authentication

  Scenario: api versions reachable pre auth on sasl listener
    Given an unauthenticated connection to a PLAIN SASL listener
    When ApiVersions is requested before authentication
    Then ApiVersions succeeds and advertises SaslHandshake and SaslAuthenticate

  Scenario: metadata rejected pre auth on sasl listener
    Given an unauthenticated connection to a PLAIN SASL listener
    When Metadata version 12 is sent before authentication
    Then the SASL connection closes without a next response frame

  Scenario: unsupported mechanism answers 33 then closes
    Given an unauthenticated connection to a PLAIN SASL listener
    When the client selects unsupported mechanism GSSAPI
    Then SaslHandshake reports UNSUPPORTED_SASL_MECHANISM and lists PLAIN
    When the client retries PLAIN on the refused connection
    Then the refused connection cannot answer another handshake
    When the client selects PLAIN on a fresh connection
    Then the fresh PLAIN handshake succeeds

  Scenario: v0 handshake runs the exchange as raw tokens
    Given a PLAIN connection after SaslHandshake version 0
    When a raw PLAIN token with valid credentials is sent
    Then the raw PLAIN server token is empty
    When the raw-token client requests Metadata after authentication
    Then the raw-token authenticated client receives Metadata

  Scenario: v0 raw token with bad credentials closes without response
    Given a PLAIN connection after SaslHandshake version 0
    When a raw PLAIN token with invalid credentials is sent
    Then the SASL connection closes without a next response frame

  Scenario: api versions after the handshake answers 34 then closes
    Given a PLAIN connection after SaslHandshake version 1
    When ApiVersions version 3 arrives after the SASL handshake
    Then ApiVersions returns only ILLEGAL_SASL_STATE
    When the SASL client reads the connection
    Then the SASL connection closes without a next response frame

  Scenario Outline: a request after the handshake answers illegal sasl state when kafka can parse it
    Given the PLAIN listener delays failed authentication by 500 milliseconds
    And the client has selected PLAIN with SaslHandshake v1
    When the client sends <request> instead of SaslAuthenticate
    Then the SASL gate applies <expected>

    Examples:
      | request                               | expected                   |
      | Metadata v12                          | delayed-ILLEGAL_SASL_STATE |
      | Produce v9                            | delayed-ILLEGAL_SASL_STATE |
      | ListOffsets v7                        | delayed-ILLEGAL_SASL_STATE |
      | CreateTopics v7                       | delayed-ILLEGAL_SASL_STATE |
      | ApiVersions at an unsupported version | delayed-ILLEGAL_SASL_STATE |
      | Produce with acks 0                   | immediate-close            |
      | Metadata at an unsupported version    | immediate-close            |
      | Metadata with a truncated body        | immediate-close            |
      | an api key Kafka 4.3.1 lacks           | immediate-close            |
