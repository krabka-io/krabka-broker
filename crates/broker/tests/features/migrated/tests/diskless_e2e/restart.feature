Feature: tests diskless e2e restart

  Scenario: a broker crashed mid flush loses no acknowledged offset
    Then diskless e2e restart: a broker crashed mid flush loses no acknowledged offset
