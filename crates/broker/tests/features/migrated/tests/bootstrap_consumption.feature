Feature: tests bootstrap consumption

  Scenario: bootstrap records provisions scram user
    Then bootstrap consumption: bootstrap records provisions scram user

  Scenario: corrupt bootstrap refuses start
    Then bootstrap consumption: corrupt bootstrap refuses start

  Scenario: bootstrap absent legacy path
    Then bootstrap consumption: bootstrap absent legacy path
