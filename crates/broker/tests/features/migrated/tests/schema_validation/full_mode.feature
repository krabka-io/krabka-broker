Feature: Full schema validation checks the body as well as the schema identity
  Scenario: full mode checks the body and id mode does not
    Given a registry binds schema 42 to validated and validated-full subjects
    And validated-full checks the complete schema body
    And the record names schema 42 but contains six invalid Avro bytes
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 0 and its log end is 1
    When the producer sends the prepared record to validated-full
    Then schema produce to validated-full returns error 87 and its log end is 0
    When the producer sends a valid Avro Order to validated-full
    Then schema produce to validated-full returns error 0 and its log end is 1
