Feature: File-configured unsecured OAuth policies enforce Kafka scope options

  Scenario: No required scope admits a token without scope
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Every required item admits
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_required_scope = "a b"
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {"scope":["b","a","c"]}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: A missing required item refuses insufficient scope
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_required_scope = "a b"
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {"scope":["a"]}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the OAuth policy returns RFC 7628 challenge {"status":"insufficient_scope", "scope":"[a, b]"}

  Scenario: Blank required scope requires nothing
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_required_scope = "   "
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Custom scope claim is read
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_required_scope = "a"
      unsecured_scope_claim_name = "scp"
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {"scp":"a"}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the configured OAuth policy admits principal alice

  Scenario: Default scope claim is ignored with a custom name
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_required_scope = "a"
      unsecured_scope_claim_name = "scp"
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {"scope":"a"}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the OAuth policy returns RFC 7628 challenge {"status":"insufficient_scope", "scope":"[a]"}

  Scenario: Numeric custom claim is an invalid token
    Given the OAuth validator is configured by this file:
      """
      [oauthbearer]
      allow_unsecured = true
      unsecured_scope_claim_name = "scp"
      """
    And alice presents an unsecured token valid for sixty seconds with extra JSON claims {"scp":7}
    When the configured OAuth validator validates the token at timestamp 2000000000000
    Then the OAuth policy returns RFC 7628 challenge {"status":"invalid_token"}
