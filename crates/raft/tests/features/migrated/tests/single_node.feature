Feature: tests single node

  Scenario: single voter create topic round trip
    Then single node: single voter create topic round trip

  Scenario: single voter duplicate topic rejected
    Then single node: single voter duplicate topic rejected
