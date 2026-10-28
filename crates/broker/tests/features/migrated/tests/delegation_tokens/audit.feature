Feature: Token mutation audit records identify the token and redact its authentication secret
  Scenario: delegation token mutations reach the audit topic
    Given a token broker has alice wonderland and bob builder with a 7 day ceiling and 24 hour renew period
    And alice authenticates with PLAIN password wonderland
    When alice mints token audited for owner caller with renewers none and lifetime -1
    Then token creation reports error 0
    When alice renews token audited for 2592000000 milliseconds
    Then token renewal reports error 0
    When alice renews an unknown HMAC of 32 bytes 0x5a for 3600000 milliseconds
    Then renewal against the unknown HMAC fails
    When alice expires token audited with period -1
    Then token expiry reports error 0
    And the audit topic names token audited and alice in all three mutations audits one renewal and leaks no HMAC
