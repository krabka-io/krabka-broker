Feature: File-configured OAuth token expiry follows Kafka clock-skew defaults

  Scenario: Unsecured default refuses one second past expiry
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      """
    And alice presents a token that expired 1 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy refuses the expired token

  Scenario: Explicit unsecured skew admits one second past expiry
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      allowable_clock_skew_ms = 5000
      """
    And alice presents a token that expired 1 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Explicit unsecured skew refuses past its limit
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      allowable_clock_skew_ms = 5000
      """
    And alice presents a token that expired 6 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy refuses the expired token

  Scenario: Signed default admits one second past expiry
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      jwks_endpoint_uri = "https://idp.example/jwks"
      """
    And alice presents a token that expired 1 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Signed default admits just inside thirty seconds
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      jwks_endpoint_uri = "https://idp.example/jwks"
      """
    And alice presents a token that expired 29 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Signed default refuses past thirty seconds
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      jwks_endpoint_uri = "https://idp.example/jwks"
      """
    And alice presents a token that expired 31 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy refuses the expired token

  Scenario: Explicit zero signed skew refuses one second past expiry
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      jwks_endpoint_uri = "https://idp.example/jwks"
      allowable_clock_skew_ms = 0
      """
    And alice presents a token that expired 1 seconds before timestamp 2000000000000
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy refuses the expired token
