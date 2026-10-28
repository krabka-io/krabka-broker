Feature: src sim playground

  Scenario: tests: interactive bootstrap elects one leader
    Then sim playground tests: interactive bootstrap elects one leader

  Scenario: tests: interactive partition then heal keeps one leader
    Then sim playground tests: interactive partition then heal keeps one leader

  Scenario: tests: drop next removes a message and records it
    Then sim playground tests: drop next removes a message and records it

  Scenario: tests: accessors and bus faults report consistently
    Then sim playground tests: accessors and bus faults report consistently

  Scenario: tests: append targets the current leader
    Then sim playground tests: append targets the current leader
