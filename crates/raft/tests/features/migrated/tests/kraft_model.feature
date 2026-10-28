Feature: tests kraft model

  Scenario: three voters election safety
    Then kraft model: three voters election safety

  Scenario: two voters linearizable
    Then kraft model: two voters linearizable

  Scenario: three voters faults
    Then kraft model: three voters faults

  Scenario: three voters append
    Then kraft model: three voters append

  Scenario: two voters append via linearizable
    Then kraft model: two voters append via linearizable
