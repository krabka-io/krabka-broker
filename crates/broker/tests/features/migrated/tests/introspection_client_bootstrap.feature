Feature: OAuth introspection client startup before the broker
  The operator applies TLS trust configuration before Broker::start can install
  a process crypto provider. The introspection client must initialize its own
  provider at that point.

  Scenario: Applying trusted introspection configuration succeeds in a fresh process
    Given a fresh process has an introspection client configuration with a secret and trusted CA
    When the listener configuration builds its introspection client before broker startup
    Then the broker uses an introspection validator without an externally installed crypto provider
