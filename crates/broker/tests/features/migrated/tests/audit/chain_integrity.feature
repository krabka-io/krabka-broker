Feature: tests audit chain integrity

  Scenario: signed checkpoints appear on audit topic
    Then audit chain integrity: signed checkpoints appear on audit topic

  Scenario: audit chain continues across restart
    Then audit chain integrity: audit chain continues across restart
