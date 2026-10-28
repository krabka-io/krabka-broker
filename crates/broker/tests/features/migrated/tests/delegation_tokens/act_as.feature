Feature: tests delegation tokens act as

  Scenario: act as super user mints token owned by target
    Then delegation tokens act as: act as super user mints token owned by target

  Scenario: act as non super user rejected with authorization failed
    Then delegation tokens act as: act as non super user rejected with authorization failed
