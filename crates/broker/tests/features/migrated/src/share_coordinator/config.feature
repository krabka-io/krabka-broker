Feature: Share coordinator persistence defaults follow Kafka
  Unconfigured brokers use the interoperable state topic and snapshot policies.

  Scenario: Select the default share coordinator persistence policy
    Given a broker has no share coordinator configuration overrides
    When the broker selects its share coordinator persistence settings
    Then the share state topic, recovery buffers, snapshots and worker policy use Kafka defaults
