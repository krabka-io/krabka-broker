Feature: Fetch session eviction preserves healthy and privileged sessions
  A newcomer can displace only a stale or lower priority session under Kafka KIP-227.

  Scenario: an empty cache has no victim
    Given an empty fetch session eviction index
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 0
    Then no fetch session can be evicted

  Scenario: a session unused for more than the minimum is stale, and goes
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 9 partitions at second 0
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 121
    Then fetch session 1 is selected for eviction

  Scenario: a session unused for exactly the minimum is not stale
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 9 partitions at second 0
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 120
    Then no fetch session can be evicted

  Scenario: a stale follower session goes to a consumer newcomer too
    Given an empty fetch session eviction index
    And cached session 3 has follower privilege true and 9 partitions at second 0
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 121
    Then fetch session 3 is selected for eviction

  Scenario: staleness picks the session unused the longest
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 1 partitions at second 0
    And cached session 2 has follower privilege false and 1 partitions at second 10
    And session 1 is used with 1 partitions at second 20
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 131
    Then fetch session 2 is selected for eviction

  Scenario: a consumer never displaces a recently used session of its own class
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 1 partitions at second 0
    And cached session 2 has follower privilege false and 1 partitions at second 1
    When a newcomer with follower privilege false and 100 partitions seeks eviction at second 2
    Then no fetch session can be evicted

  Scenario: a consumer never displaces a session created within the minimum
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 1 partitions at second 100
    And session 1 is used with 1 partitions at second 110
    When a newcomer with follower privilege false and 100 partitions seeks eviction at second 115
    Then no fetch session can be evicted

  Scenario: a larger consumer displaces an older, smaller consumer session
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 2 partitions at second 0
    And session 1 is used with 2 partitions at second 121
    When a newcomer with follower privilege false and 3 partitions seeks eviction at second 122
    Then fetch session 1 is selected for eviction

  Scenario: an equally large consumer does not
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 2 partitions at second 0
    And session 1 is used with 2 partitions at second 121
    When a newcomer with follower privilege false and 2 partitions seeks eviction at second 122
    Then no fetch session can be evicted

  Scenario: a smaller consumer does not
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 2 partitions at second 0
    And session 1 is used with 2 partitions at second 121
    When a newcomer with follower privilege false and 1 partitions seeks eviction at second 122
    Then no fetch session can be evicted

  Scenario: a consumer displaces the smallest old consumer session, not the oldest
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 5 partitions at second 0
    And cached session 2 has follower privilege false and 3 partitions at second 1
    And session 1 is used with 5 partitions at second 121
    And session 2 is used with 3 partitions at second 122
    When a newcomer with follower privilege false and 9 partitions seeks eviction at second 123
    Then fetch session 2 is selected for eviction

  Scenario: a consumer never displaces a follower session, however old
    Given an empty fetch session eviction index
    And cached session 3 has follower privilege true and 1 partitions at second 0
    And session 3 is used with 1 partitions at second 121
    When a newcomer with follower privilege false and 1000 partitions seeks eviction at second 122
    Then no fetch session can be evicted

  Scenario: a follower displaces any consumer session, however small the follower
    Given an empty fetch session eviction index
    And cached session 1 has follower privilege false and 50 partitions at second 0
    And cached session 3 has follower privilege true and 1 partitions at second 1
    When a newcomer with follower privilege true and 1 partitions seeks eviction at second 2
    Then fetch session 1 is selected for eviction

  Scenario: a follower displaces an old follower session only when it is larger
    Given an empty fetch session eviction index
    And cached session 3 has follower privilege true and 2 partitions at second 0
    And session 3 is used with 2 partitions at second 121
    When a newcomer with follower privilege true and 2 partitions seeks eviction at second 122
    Then no fetch session can be evicted
