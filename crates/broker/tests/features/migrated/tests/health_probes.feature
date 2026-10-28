Feature: tests health probes

  Scenario: readyz is 503 during startup and 200 once the broker is up
    Then health probes: readyz is 503 during startup and 200 once the broker is up

  Scenario: readyz names each condition as startup advances
    Then health probes: readyz names each condition as startup advances

  Scenario: readyz stays 200 while the broker runs
    Then health probes: readyz stays 200 while the broker runs

  Scenario: readyz is 503 when shutting down
    Then health probes: readyz is 503 when shutting down

  Scenario: shutting down outranks every other not ready condition
    Then health probes: shutting down outranks every other not ready condition
