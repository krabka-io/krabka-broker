Feature: External OPA decisions control ordinary authenticated writes
  Scenario: OPA denial prevents an ordinary authenticated topic write
    Given OPA returns false while the administrator creates topic blocked-topic
    When ordinary PLAIN user alice produces hello to the administrator's topic
    Then the partition returns topic authorization failed

  Scenario: OPA permission admits an ordinary authenticated topic write
    Given OPA returns true while the administrator creates topic permitted-topic
    When ordinary PLAIN user alice produces hello to the administrator's topic
    Then the partition accepts the authorized write
