Feature: tests admin handlers admin listings

  Scenario: list config resources default set includes topics and brokers
    Then admin handlers admin listings: list config resources default set includes topics and brokers

  Scenario: list groups includes freshly created group
    Then admin handlers admin listings: list groups includes freshly created group
