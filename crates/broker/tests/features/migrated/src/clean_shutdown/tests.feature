Feature: Kafka clean-shutdown proof compatibility
  A graceful shutdown leaves Kafka's exact version-zero document and filename.
  Missing fields, legacy bare epochs, null epochs, and junk never prove a restart.

  Scenario: A graceful shutdown writes Kafka's version-zero epoch document
    Given a broker at epoch 42 has a fresh log directory
    When the graceful shutdown writes and encodes its restart proof
    Then both the encoder and .kafka_cleanshutdown contain exactly:
      """
      {"version":0,"brokerEpoch":42}
      """

  Scenario: A legacy bare epoch is not a versioned proof
    Given the supplied clean-shutdown document is:
      """
      42
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed

  Scenario: A proof without a version is malformed
    Given the supplied clean-shutdown document is:
      """
      {"brokerEpoch":42}
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed

  Scenario: A proof without a broker epoch is malformed
    Given the supplied clean-shutdown document is:
      """
      {"version":0}
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed

  Scenario: A null broker epoch is malformed
    Given the supplied clean-shutdown document is:
      """
      {"version":0,"brokerEpoch":null}
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed

  Scenario: Arbitrary text is not a clean-shutdown proof
    Given the supplied clean-shutdown document is:
      """
      not-an-epoch
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed

  Scenario: An empty document is not a clean-shutdown proof
    Given the supplied clean-shutdown document is:
      """
      """
    When the broker decodes that clean-shutdown document
    Then the clean-shutdown document is rejected as malformed
