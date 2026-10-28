Feature: Production Kafka feature discovery

  Scenario: v3 response advertises supported and bootstrapped finalized features
    Given a fresh standalone broker at the release feature defaults
    When a client negotiates ApiVersions
    Then the feature negotiation succeeds
    And the supported feature levels are:
      | feature             | minimum | maximum           |
      | metadata.version    | 7       | latest-production |
      | group.version       | 0       | 1                 |
      | transaction.version | 0       | 2                 |
      | krabka.version      | 0       | 2                 |
    And the bootstrapped finalized levels are:
      | feature             | level             |
      | metadata.version    | latest-production |
      | group.version       | 1                 |
      | transaction.version | 2                 |
    And krabka.version is omitted from finalized features until explicitly enabled
    And the finalized feature epoch is nonnegative
