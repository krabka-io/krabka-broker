Feature: tests soak drift

  Scenario: tests: a flat series is level
    Then soak drift tests: a flat series is level

  Scenario: tests: noise around a flat mean is level
    Then soak drift tests: noise around a flat mean is level

  Scenario: tests: a rising series drifts
    Then soak drift tests: a rising series drifts

  Scenario: tests: a segment roll sawtooth is level
    Then soak drift tests: a segment roll sawtooth is level

  Scenario: tests: a sawtooth on a rising baseline drifts
    Then soak drift tests: a sawtooth on a rising baseline drifts

  Scenario: tests: a ceiling breach names the sample
    Then soak drift tests: a ceiling breach names the sample

  Scenario: tests: the ceiling outranks a flat trend
    Then soak drift tests: the ceiling outranks a flat trend

  Scenario: tests: too few samples is not a pass
    Then soak drift tests: too few samples is not a pass

  Scenario: tests: a falling series is level
    Then soak drift tests: a falling series is level

  Scenario: tests: the floor protects a near zero series
    Then soak drift tests: the floor protects a near zero series

  Scenario: tests: judge all reports one verdict per series
    Then soak drift tests: judge all reports one verdict per series
