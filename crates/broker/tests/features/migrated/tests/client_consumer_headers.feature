Feature: Consumer records preserve producer headers
  Scenario: consumer record carries headers
    Given topic h and a record with value v and header trace equal to abc
    When the producer sends and flushes the record
    And an Earliest consumer in group g reads the first record
    Then the consumed record has exactly the trace header with value abc
