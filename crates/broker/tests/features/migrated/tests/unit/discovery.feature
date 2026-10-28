Feature: Client discovery on a single Kafka broker
  Scenario: api versions round trip
    Given a single broker for client discovery
    When the client requests supported API versions
    Then API version discovery succeeds and advertises ApiVersions itself

  Scenario: metadata returns this broker and listed topics
    Given a single broker for client discovery
    And topic beta has three partitions
    When the client requests metadata for all topics
    Then metadata names this broker and all three beta partition leaders

  Scenario: find coordinator creates the offsets topic then returns self
    Given a single broker for client discovery
    When the client first looks up coordinator for any-group
    Then the group coordinator is initially unavailable with no broker address
    When the client retries group coordinator discovery until it is ready
    Then the group coordinator is this broker at its bound listener
