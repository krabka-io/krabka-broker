Feature: tests admin handlers admin delete records

  Scenario: delete records trims log start
    Then admin handlers admin delete records: delete records trims log start

  Scenario: a trim inside the active segment survives a restart
    Then admin handlers admin delete records: a trim inside the active segment survives a restart
