Feature: UnregisterBroker removes registered endpoints and reports missing registrations

  Background:
    Given a running broker registered as node 1

  Scenario: unregister known broker drops it from metadata
    When an administrator unregisters broker 1
    Then the unregister request succeeds without an error message
    And the committed controller image no longer contains broker 1

  Scenario Outline: unregister unknown broker returns broker id not registered
    When an administrator unregisters broker <broker_id>
    Then the complete unregister response reports the requested broker ID is not registered

    Examples:
      | broker_id |
      | 999       |
      | -1        |

  Scenario: unregister is idempotent on repeat call
    When an administrator unregisters broker 1
    Then the unregister request succeeds without an error message
    And the committed controller image no longer contains broker 1
    When an administrator unregisters broker 1
    Then the repeated unregister returns BROKER_ID_NOT_REGISTERED
