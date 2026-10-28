Feature: Omitted idle windows retain Kafka default configuration provenance

  Scenario: No broker or listener override preserves the default idle window
    Given the listener configuration file names broker 0 and no idle-window settings
    When the listener configuration is parsed and applied to the broker
    Then the idle window remains unsupplied and all listeners use Kafka's default without overrides
