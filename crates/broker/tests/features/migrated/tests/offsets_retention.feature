Feature: tests offsets retention

  Scenario: describe configs reports the retention knobs
    Then offsets retention: describe configs reports the retention knobs

  Scenario: describe configs reports a named knob as static
    Then offsets retention: describe configs reports a named knob as static

  Scenario: the broker sweep reaps a dead group on its own
    Then offsets retention: the broker sweep reaps a dead group on its own
