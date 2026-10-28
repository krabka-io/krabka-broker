Feature: Schema-bound records and tombstones pass topic validation
  Scenario: a record framed with a bound schema id is accepted
    Given a registry binds schema 42 to validated and validated-full subjects
    And the record carries schema 42 with arbitrary body bytes
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 0 and its log end is 1

  Scenario: the cache counters move on a validated produce
    Given a registry binds schema 42 to validated and validated-full subjects
    And the record carries schema 42 with arbitrary body bytes
    When an observer reads the schema-validation cache counters
    Then schema cache misses are 0 and hits are 0
    When the producer sends the prepared record to validated
    Then the validated produce succeeds
    And schema cache misses are 1 and hits are 0
    When the producer sends the prepared record to validated
    Then the validated produce succeeds
    And schema cache misses are 1 and hits are 1

  Scenario: a tombstone is accepted on a validated topic
    Given a registry binds schema 42 to validated and validated-full subjects
    And the record value is a tombstone
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 0 and its log end is 1
