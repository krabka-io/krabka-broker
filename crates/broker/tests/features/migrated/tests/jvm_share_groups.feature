Feature: tests jvm share groups

  Scenario: jvm share consumer reads krabka
    Then jvm share groups: jvm share consumer reads krabka

  Scenario: jvm share groups describe state
    Then jvm share groups: jvm share groups describe state

  Scenario: jvm share groups list
    Then jvm share groups: jvm share groups list

  Scenario: jvm share consumer by duration skips records outside the window
    Then jvm share groups: jvm share consumer by duration skips records outside the window

  Scenario: jvm share consumer defaults to latest
    Then jvm share groups: jvm share consumer defaults to latest
