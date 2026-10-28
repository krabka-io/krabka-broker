Feature: Schema validation rejects invalid records before appending
  Scenario Outline: records that fail validation are rejected and not appended
    Given a registry binds schema 42 to validated and validated-full subjects
    And the record value has defect <defect>
    When the producer sends the prepared record to validated
    Then the invalid record identifies batch index zero and leaves validated empty

    Examples:
      | defect                            |
      | unframed text                     |
      | bad magic byte                    |
      | truncated schema id               |
      | unregistered schema 99            |
      | schema 43 bound to another subject|

  Scenario: an unvalidated topic accepts what a validated one rejects
    Given a registry binds schema 42 to validated and validated-full subjects
    And the same broker has an unvalidated control topic
    And the record value has defect unframed text
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 87 and its log end is 0
    When the producer sends the prepared record to control
    Then schema produce to control returns error 0 and its log end is 1

  Scenario: the rejected record is named by its index in the batch
    Given a registry binds schema 42 to validated and validated-full subjects
    When the producer sends a batch whose first record is valid and second is unframed
    Then the whole batch is rejected and the sole per-record error identifies index one
