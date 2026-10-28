Feature: TLS listener negotiation and endpoint discovery
  Scenario: tls listener accepts tls handshake only
    Given an SSL listener and a client pinning its development certificate
    When the configured TLS broker starts
    And the pinned client negotiates TLS with the SSL listener
    Then the pinned client has an established TLS stream

  Scenario: metadata response carries listener endpoints
    Given a broker configured with PLAINTEXT and SSL listeners
    When the configured TLS broker starts
    Then self registration contains exactly PLAINTEXT and SSL with their protocols
    When a client requests Metadata through the PLAINTEXT listener
    Then Metadata includes this broker with node id 1
