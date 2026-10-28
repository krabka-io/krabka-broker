Feature: Legacy Produce messages convert into modern fetchable records
  Scenario: A version zero message set is readable as a modern record batch
    Given a legacy compatible broker hosts a single partition legacy v0 topic
    When a raw Produce version zero appends key k and value v
    Then the version zero response acknowledges exactly one topic without an error
    When a modern typed client fetches the converted record
    Then the successful fetch contains one v2 record retaining key k and value v
