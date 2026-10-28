Feature: Delegation token expiration and deletion
  Negative periods delete even an expired token. Nonnegative periods require a
  live token and saturate at its maximum timestamp.

  Scenario: Expiration either deletes the token or updates a bounded live deadline (1: boundary zero period at zero, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 0 ms for a period of 0 ms
    Then token expiration is updated with deadline 0 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (2: boundary zero period, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 0 ms
    Then token expiration is updated with deadline 100 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (3: boundary positive period, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 25 ms
    Then token expiration is updated with deadline 125 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (4: boundary period capped at max, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 500 ms
    Then token expiration is updated with deadline 200 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (5: boundary near overflow is capped, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 9223372036854775707 ms
    Then token expiration is updated with deadline 200 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (6: boundary saturating overflow, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp maximum ms
    When token expiration is requested at 100 ms for a period of maximum ms
    Then token expiration is updated with deadline maximum ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (7: boundary zero period at expiry, current 100)
    Given an expirable token with current deadline 100 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 0 ms
    Then token expiration is updated with deadline 100 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (8: boundary zero period at max, current 100)
    Given an expirable token with current deadline 100 ms and maximum timestamp 100 ms
    When token expiration is requested at 100 ms for a period of 0 ms
    Then token expiration is updated with deadline 100 ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (9: boundary negative period deletes live, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of -1 ms
    Then token expiration is delete with deadline none ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (10: boundary negative period deletes expired, current 50)
    Given an expirable token with current deadline 50 ms and maximum timestamp 60 ms
    When token expiration is requested at 100 ms for a period of -1 ms
    Then token expiration is delete with deadline none ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (11: boundary minimum period deletes, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of minimum ms
    Then token expiration is delete with deadline none ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (12: boundary current expiry is past, current 99)
    Given an expirable token with current deadline 99 ms and maximum timestamp 200 ms
    When token expiration is requested at 100 ms for a period of 0 ms
    Then token expiration is expired with deadline none ms

  Scenario: Expiration either deletes the token or updates a bounded live deadline (13: boundary max timestamp is past, current 150)
    Given an expirable token with current deadline 150 ms and maximum timestamp 99 ms
    When token expiration is requested at 100 ms for a period of 0 ms
    Then token expiration is expired with deadline none ms
