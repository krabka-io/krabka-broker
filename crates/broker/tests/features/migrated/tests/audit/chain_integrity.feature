Feature: Audit checkpoints sign a chain that survives broker restart
  Scenario: signed checkpoints appear on audit topic
    Given an Ed25519 audit signing key k-test with a checkpoint after every event
    When the admin creates cp-topic
    Then a chained creation and signed checkpoint k-test become durable on the audit topic

  Scenario: audit chain continues across restart
    Given a broker created r1 and shut down with its audit log retained
    When the broker restarts from the same log and creates r2
    And a client reads at least four chained audit sequence numbers
    Then the audit sequences are contiguous from zero and contain no duplicates
