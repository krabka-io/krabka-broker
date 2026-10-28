Feature: tests audit emission

  Scenario: audit topic exists after startup
    Then audit emission: audit topic exists after startup

  Scenario: broker started event is written to audit topic
    Then audit emission: broker started event is written to audit topic

  Scenario: every admin mutation is audited
    Then audit emission: every admin mutation is audited

  Scenario: sasl plain logins are audited either way
    Then audit emission: sasl plain logins are audited either way
