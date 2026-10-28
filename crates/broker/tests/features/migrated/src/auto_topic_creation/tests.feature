Feature: src auto topic creation tests

  Scenario: creatable topic follows kafkas creatable topic
    Then auto topic creation tests: creatable topic follows kafkas creatable topic

  Scenario: the dead letter topic is created as kafka creates it
    Then auto topic creation tests: the dead letter topic is created as kafka creates it

  Scenario: a finished streams creation caches kafkas errors
    Then auto topic creation tests: a finished streams creation caches kafkas errors
