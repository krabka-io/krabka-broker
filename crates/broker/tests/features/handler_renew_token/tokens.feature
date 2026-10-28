Feature: RenewDelegationToken preserves Kafka authorization ordering and deadline semantics
  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (1)
    Given the renew request names a live token using alice delegation-token authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 64 expiry -1 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (2)
    Given the renew request names a live token using alice delegation-token authentication with secret absent and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 64 expiry -1 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=password (3)
    Given the renew request names a live token using alice password authentication with secret absent and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 61 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=missing, caller=alice, credential=password (4)
    Given the renew request names a missing token using alice password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 62 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=wrong-secret, caller=alice, credential=password (5)
    Given the renew request names a wrong-secret token using alice password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 62 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=eve, credential=password (6)
    Given the renew request names a live token using eve password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=admin, credential=password (7)
    Given the renew request names a live token using admin password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=minter, credential=password (8)
    Given the renew request names a live token using minter password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 63 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=eve, credential=password (9)
    Given the renew request names a expired token using eve password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 66 expiry 0 and leaves both token deadlines unchanged

  Scenario: Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=alice, credential=password (10)
    Given the renew request names a expired token using alice password authentication with secret configured and period 1000
    When the controller handles the renew token request
    Then the renew refusal has error 66 expiry 0 and leaves both token deadlines unchanged

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (1)
    Given alice may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period 3600000
    When the controller handles the renew token request
    Then the renew expiry follows 3600000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=3600000, maximum=86400000 (2)
    Given bob may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period 3600000
    When the controller handles the renew token request
    Then the renew expiry follows 3600000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (3)
    Given alice may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period 36000000
    When the controller handles the renew token request
    Then the renew expiry follows 7200000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (4)
    Given alice may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period -1
    When the controller handles the renew token request
    Then the renew expiry follows 7200000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (5)
    Given alice may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period 0
    When the controller handles the renew token request
    Then the renew expiry follows 7200000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (6)
    Given alice may renew a token expiring in 3600000 milliseconds with maximum lifetime 86400000 milliseconds using period -5
    When the controller handles the renew token request
    Then the renew expiry follows 7200000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=72000000, maximum=86400000 (7)
    Given alice may renew a token expiring in 72000000 milliseconds with maximum lifetime 86400000 milliseconds using period 60000
    When the controller handles the renew token request
    Then the renew expiry follows 60000 milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=1800000 (8)
    Given alice may renew a token expiring in 60000 milliseconds with maximum lifetime 1800000 milliseconds using period -1
    When the controller handles the renew token request
    Then the renew expiry follows maximum milliseconds and the token is stored

  Scenario: Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=1800000 (9)
    Given alice may renew a token expiring in 60000 milliseconds with maximum lifetime 1800000 milliseconds using period 9223372036854775807
    When the controller handles the renew token request
    Then the renew expiry follows maximum milliseconds and the token is stored

