Feature: tests kip516 metadata

  Scenario: metadata unknown topic id returns unknown topic id
    Then kip516 metadata: metadata unknown topic id returns unknown topic id

  Scenario: metadata name and id of different topics describes the id
    Then kip516 metadata: metadata name and id of different topics describes the id
