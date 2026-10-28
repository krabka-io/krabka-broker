Feature: tests share state

  Scenario: find coordinator share creates topic and returns broker
    Then share state: find coordinator share creates topic and returns broker

  Scenario: persister round trip
    Then share state: persister round trip

  Scenario: write fences stale state epoch
    Then share state: write fences stale state epoch

  Scenario: state survives restart
    Then share state: state survives restart
