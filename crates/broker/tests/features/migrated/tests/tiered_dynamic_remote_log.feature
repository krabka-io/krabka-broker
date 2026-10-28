Feature: tests tiered dynamic remote log

  Scenario: the remote fetch quota holds back cold reads until it is raised
    Then tiered dynamic remote log: the remote fetch quota holds back cold reads until it is raised

  Scenario: the remote copy quota holds back uploads until it is raised
    Then tiered dynamic remote log: the remote copy quota holds back uploads until it is raised
