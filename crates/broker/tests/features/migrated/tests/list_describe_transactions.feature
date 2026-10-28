Feature: tests list describe transactions

  Scenario: list transactions returns ongoing txn
    Then list describe transactions: list transactions returns ongoing txn

  Scenario: list transactions state filter excludes non matching
    Then list describe transactions: list transactions state filter excludes non matching

  Scenario: list transactions reports unknown state filters
    Then list describe transactions: list transactions reports unknown state filters

  Scenario: describe transactions returns full state for known tid
    Then list describe transactions: describe transactions returns full state for known tid

  Scenario: describe transactions returns not found for unknown tid
    Then list describe transactions: describe transactions returns not found for unknown tid
