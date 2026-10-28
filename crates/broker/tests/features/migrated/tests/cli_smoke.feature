Feature: tests cli smoke

  Scenario: help mentions cluster id and advertised listener
    Then cli smoke: help mentions cluster id and advertised listener

  Scenario: version returns zero
    Then cli smoke: version returns zero

  Scenario: boots with config file listener
    Then cli smoke: boots with config file listener

  Scenario: errors when config file and listen addr both set
    Then cli smoke: errors when config file and listen addr both set

  Scenario: refuses a log dir formatted for another node
    Then cli smoke: refuses a log dir formatted for another node

  Scenario: each role opens only its own listeners
    Then cli smoke: each role opens only its own listeners
