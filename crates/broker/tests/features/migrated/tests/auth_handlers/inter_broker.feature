Feature: Authenticating outbound inter-broker connections

  Scenario: inter broker client authenticates via plain
    Given a SASL_PLAINTEXT peer accepting the broker PLAIN principal
    And InterBrokerClient is configured with that principal's PLAIN credentials
    When InterBrokerClient connects and sends ApiVersions on the returned authenticated stream
    Then the authenticated inter-broker stream returns a decodable ApiVersions response
