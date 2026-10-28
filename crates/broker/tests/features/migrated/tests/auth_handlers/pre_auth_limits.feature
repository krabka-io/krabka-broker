Feature: SASL frame limits, failure delays and pre-authentication routing

  Scenario: a frame over the sasl receive limit fails the authentication
    Given a PLAIN broker with a 1 KiB SASL receive limit and authentication metrics
    When the client declares a 2048 byte frame but sends only 8 body bytes
    Then the unauthenticated socket closes without a response frame
    When the pre-authentication metrics are scraped
    Then the Unknown-mechanism failed-authentication counter equals 1

  Scenario: the larger request limit returns once the peer authenticated
    Given a PLAIN broker with a 1 KiB SASL receive limit
    And a Metadata request for 64 topics exceeds 2048 encoded bytes
    And PLAIN has been selected but authentication is incomplete
    When a 2048 byte SaslAuthenticate body is sent mid-exchange
    Then the unauthenticated socket closes without a response frame
    Given admin authenticates on a fresh PLAIN socket
    When the authenticated socket sends that large Metadata request
    Then the authenticated Metadata response contains all 64 requested topics

  Scenario Outline: a failed authentication holds its answer for the configured delay
    Given a PLAIN broker with failed-authentication delay <delay> milliseconds
    When admin attempts PLAIN authentication with a wrong password
    Then the failed authentication honors the configured minimum delay
    And the unauthenticated socket closes without a response frame

    Examples:
      | delay |
      | 600   |
      | 0     |

  Scenario: a second api versions before the handshake closes the connection
    Given a PLAIN broker before any ApiVersions negotiation
    And a fresh unauthenticated socket to that PLAIN broker
    When the client sends an unsupported ApiVersions request
    Then the ApiVersions request returns error 35
    When ApiVersions v3 client name not valid! is negotiated
    Then the ApiVersions request returns error 42
    When ApiVersions v3 client name krabka-test is negotiated
    Then the ApiVersions request returns error 0
    When admin authenticates on the current PLAIN socket
    Then PLAIN authentication succeeds after the negotiation
    Given a fresh unauthenticated socket to that PLAIN broker
    When ApiVersions v3 client name krabka-test is negotiated
    Then the ApiVersions request returns error 0
    When a second valid ApiVersions frame is sent before a handshake
    Then the unauthenticated socket closes without a response frame

  Scenario: a pre auth api versions carries no rebootstrap check
    Given a PLAIN broker with unstable API versions enabled
    And the requested cluster id is stale and node id is 41
    And a fresh unauthenticated socket to that PLAIN broker
    When ApiVersions v5 is requested with that stale cluster identity
    Then the ApiVersions request returns error 0
    And the pre-authentication ApiVersions response advertises supported APIs
    When a fresh client sends that cluster id without a node id
    Then the ApiVersions request returns error 42
    When admin authenticates on the current PLAIN socket
    Then PLAIN authentication succeeds after the negotiation
    When ApiVersions v5 is requested with that stale cluster identity
    Then the authenticated stale identity requires rebootstrap
