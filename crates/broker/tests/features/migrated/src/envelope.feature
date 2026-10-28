Feature: src envelope

  Scenario: tests: a principal kafka would refuse is a deserialization failure
    Then envelope tests: a principal kafka would refuse is a deserialization failure

  Scenario: tests: each failure carries the error code kafka assigns it
    Then envelope tests: each failure carries the error code kafka assigns it

  Scenario: tests: the forwardable set is the one kafka publishes
    Then envelope tests: the forwardable set is the one kafka publishes

  Scenario: tests: an embedded request kafka would refuse gets the code kafka gives
    Then envelope tests: an embedded request kafka would refuse gets the code kafka gives
