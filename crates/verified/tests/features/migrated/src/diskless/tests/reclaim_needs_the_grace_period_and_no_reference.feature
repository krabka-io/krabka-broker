Feature: Diskless logical-range retention prefixes
  Retention applies time, size debt and delete-floor predicates to an ordered
  prefix, preserves the newest logical range, and fails closed at arithmetic limits.

  Scenario: No policy preserves every logical range
    Given diskless retention age none ms, size none bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: Time retention preserves the newest range
    Given diskless retention age 500 ms, size none bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 100 | 4 |
      | 200 | 100 | 9 |
      | 900 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 2 diskless ranges are removable

  Scenario: Even an entirely expired index preserves its newest range
    Given diskless retention age 500 ms, size none bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 100 | 4 |
      | 200 | 100 | 9 |
      | 300 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 2 diskless ranges are removable

  Scenario: Time retention stops at the first live range
    Given diskless retention age 500 ms, size none bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 100 | 4 |
      | 900 | 100 | 9 |
      | 100 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: Size debt removes only the oldest complete range
    Given diskless retention age none ms, size 150 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: Exact size debt removes one complete range
    Given diskless retention age none ms, size 200 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: Insufficient size debt keeps the first range
    Given diskless retention age none ms, size 250 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: An index fitting its size budget is preserved
    Given diskless retention age none ms, size 300 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: The delete floor removes the older range
    Given diskless retention age none ms, size none bytes, delete floor 5, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: A floor beyond the index still preserves its newest range
    Given diskless retention age none ms, size none bytes, delete floor 99, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 2 diskless ranges are removable

  Scenario: Time and floor predicates contribute to the same prefix
    Given diskless retention age 500 ms, size none bytes, delete floor 5, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 100 | 4 |
      | 100 | 100 | 9 |
      | 900 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 2 diskless ranges are removable

  Scenario: Time-expired ranges still pay size debt
    Given diskless retention age 500 ms, size 150 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 100 | 4 |
      | 900 | 100 | 9 |
      | 900 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: A delete floor equal to the last offset does not remove that range
    Given diskless retention age none ms, size none bytes, delete floor 10, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 10 | 10 |
      | 200 | 10 | 20 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: A timestamp equal to the retention horizon is retained
    Given diskless retention age 500 ms, size none bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 500 | 10 | 10 |
      | 900 | 10 | 20 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: A zero-byte range can pay zero size debt
    Given diskless retention age none ms, size 10 bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 100 | 0 | 10 |
      | 200 | 10 | 20 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 1 diskless ranges are removable

  Scenario: A retention horizon below the minimum timestamp fails closed
    Given diskless retention age 1 ms, size none bytes, delete floor 0, and current time minimum ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: A retention horizon above the maximum timestamp fails closed
    Given diskless retention age -1 ms, size none bytes, delete floor 0, and current time maximum ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
      | 20 | 100 | 9 |
      | 30 | 100 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: Summing maximum byte lengths does not overflow
    Given diskless retention age none ms, size maximum bytes, delete floor 0, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 18446744073709551615 | 4 |
      | 20 | 18446744073709551615 | 9 |
      | 30 | 18446744073709551615 | 14 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 2 diskless ranges are removable

  Scenario: A single newest range survives all deletion predicates
    Given diskless retention age 1 ms, size 0 bytes, delete floor 99, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
      | 10 | 100 | 4 |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable

  Scenario: An empty logical index has no removable ranges
    Given diskless retention age 1 ms, size 0 bytes, delete floor 99, and current time 1000 ms
    And the diskless index contains these ranges oldest first:
      | max_timestamp_ms | bytes | last_offset |
    When Kafka diskless retention evaluates the removable prefix
    Then the oldest 0 diskless ranges are removable
