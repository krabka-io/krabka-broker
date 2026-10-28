Feature: tests soak load

  Scenario: an undisturbed load reads back once and hides what it aborted
    Then soak load: an undisturbed load reads back once and hides what it aborted

  Scenario: a record stored twice is read back at both offsets and a stranger is skipped
    Then soak load: a record stored twice is read back at both offsets and a stranger is skipped
