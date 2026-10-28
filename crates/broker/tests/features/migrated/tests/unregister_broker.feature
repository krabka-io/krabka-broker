Feature: tests unregister broker

  Scenario: unregister known broker drops it from metadata
    Then unregister broker: unregister known broker drops it from metadata

  Scenario: unregister unknown broker returns broker id not registered
    Then unregister broker: unregister unknown broker returns broker id not registered

  Scenario: unregister is idempotent on repeat call
    Then unregister broker: unregister is idempotent on repeat call
