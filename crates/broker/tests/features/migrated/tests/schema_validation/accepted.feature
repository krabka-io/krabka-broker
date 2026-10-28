Feature: tests schema validation accepted

  Scenario: a record framed with a bound schema id is accepted
    Then schema validation accepted: a record framed with a bound schema id is accepted

  Scenario: the cache counters move on a validated produce
    Then schema validation accepted: the cache counters move on a validated produce

  Scenario: a tombstone is accepted on a validated topic
    Then schema validation accepted: a tombstone is accepted on a validated topic
