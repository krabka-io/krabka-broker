Feature: tests max message bytes config surface

  Scenario: create topics accepts the key and describe configs echoes it
    Then max message bytes config surface: create topics accepts the key and describe configs echoes it

  Scenario: an incremental alter raises the cap for the next produce
    Then max message bytes config surface: an incremental alter raises the cap for the next produce

  Scenario: the alter path rejects the values kafka rejects
    Then max message bytes config surface: the alter path rejects the values kafka rejects
