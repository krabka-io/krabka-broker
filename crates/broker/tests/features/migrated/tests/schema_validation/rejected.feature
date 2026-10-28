Feature: tests schema validation rejected

  Scenario: records that fail validation are rejected and not appended
    Then schema validation rejected: records that fail validation are rejected and not appended

  Scenario: an unvalidated topic accepts what a validated one rejects
    Then schema validation rejected: an unvalidated topic accepts what a validated one rejects

  Scenario: the rejected record is named by its index in the batch
    Then schema validation rejected: the rejected record is named by its index in the batch
