Feature: tests produce log append time

  Scenario: a log append time topic reports the stamp it wrote
    Then produce log append time: a log append time topic reports the stamp it wrote

  Scenario: a create time topic reports no stamp
    Then produce log append time: a create time topic reports no stamp

  Scenario: a timestamp older than the window is refused
    Then produce log append time: a timestamp older than the window is refused

  Scenario: a timestamp newer than the window is refused
    Then produce log append time: a timestamp newer than the window is refused

  Scenario: log append time ignores the timestamp windows
    Then produce log append time: log append time ignores the timestamp windows
