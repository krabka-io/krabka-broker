Feature: ExpireDelegationToken preserves Kafka authorization ordering and deadline semantics
  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (1)
    Given the expire request names a live token using alice delegation-token authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 64 expiry -1 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (2)
    Given the expire request names a live token using alice delegation-token authentication with secret absent and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 64 expiry -1 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=password (3)
    Given the expire request names a live token using alice password authentication with secret absent and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 61 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=missing, caller=alice, credential=password (4)
    Given the expire request names a missing token using alice password authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 62 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=wrong-secret, caller=alice, credential=password (5)
    Given the expire request names a wrong-secret token using alice password authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 62 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=eve, credential=password (6)
    Given the expire request names a live token using eve password authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=admin, credential=password (7)
    Given the expire request names a live token using admin password authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=minter, credential=password (8)
    Given the expire request names a live token using minter password authentication with secret configured and period -1
    When the controller handles the expire token request
    Then the expire refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=eve, credential=password (9)
    Given the expire request names a expired token using eve password authentication with secret configured and period 0
    When the controller handles the expire token request
    Then the expire refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=alice, credential=password (10)
    Given the expire request names a expired token using alice password authentication with secret configured and period 0
    When the controller handles the expire token request
    Then the expire refusal has error 66 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=bob, credential=password (11)
    Given the expire request names a expired token using bob password authentication with secret configured and period 1000
    When the controller handles the expire token request
    Then the expire refusal has error 66 expiry 0 and leaves both token deadlines unchanged

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (1)
    Given alice may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period -1
    When the controller handles the expire token request
    Then the expire expiry follows 0 milliseconds and the token is deleted

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=60000, maximum=86400000 (2)
    Given bob may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period -5
    When the controller handles the expire token request
    Then the expire expiry follows 0 milliseconds and the token is deleted

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=-1000, maximum=86400000 (3)
    Given alice may expire a token expiring in -1000 milliseconds with maximum lifetime 86400000 milliseconds using period -1
    When the controller handles the expire token request
    Then the expire expiry follows 0 milliseconds and the token is deleted

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (4)
    Given alice may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period 0
    When the controller handles the expire token request
    Then the expire expiry follows 0 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=60000, maximum=86400000 (5)
    Given bob may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period 30000
    When the controller handles the expire token request
    Then the expire expiry follows 30000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (6)
    Given alice may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period 120000
    When the controller handles the expire token request
    Then the expire expiry follows 120000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (7)
    Given alice may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period 172800000
    When the controller handles the expire token request
    Then the expire expiry follows maximum milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (8)
    Given alice may expire a token expiring in 60000 milliseconds with maximum lifetime 86400000 milliseconds using period 9223372036854775807
    When the controller handles the expire token request
    Then the expire expiry follows maximum milliseconds and the token is stored

