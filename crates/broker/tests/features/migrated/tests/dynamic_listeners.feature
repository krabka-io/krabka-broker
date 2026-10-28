Feature: Dynamic listener changes apply to live sockets and survive restart

  Scenario: an added listener serves until it is removed
    Given a running combined broker with its static PLAINTEXT listener
    When listener EXTRA using PLAINTEXT and mechanisms none is added on a free loopback port
    Then the alter returns the complete successful broker resource response
    And a fresh connection to the added listener serves ApiVersions
    When the dynamic listeners and protocol map are deleted
    Then the alter returns the complete successful broker resource response
    And the held connection closes and the added port refuses new connections
    And the original static listener still serves ApiVersions

  Scenario: an added sasl listener offers its own mechanisms
    Given a running combined broker with its static PLAINTEXT listener
    When listener SASL using SASL_PLAINTEXT and mechanisms SCRAM-SHA-256 is added on a free loopback port
    Then the alter returns the complete successful broker resource response
    And a fresh connection to the added listener serves ApiVersions
    When the held connection requests SASL mechanism SCRAM-SHA-256
    Then the SASL response has error 0 and exactly mechanism SCRAM-SHA-256

  Scenario: a stored listener opens after a restart
    Given a running combined broker with its static PLAINTEXT listener
    When listener EXTRA using PLAINTEXT and mechanisms none is added on a free loopback port
    Then the alter returns the complete successful broker resource response
    And a fresh connection to the added listener serves ApiVersions
    When the broker is shut down while its dynamic listener remains configured
    Then the added port refuses new connections
    When the broker rejoins using the same data directory
    Then a fresh connection to the added listener serves ApiVersions

  Scenario: a listener change kafka refuses is refused
    Given a running combined broker with its static PLAINTEXT listener
    When the advertised PLAINTEXT listener is removed from the static listener list
    Then the complete broker resource response has error 42 and message
      """
      requirement failed: advertised.listeners listener names must be equal to or a subset of the ones defined in listeners. Found PLAINTEXT. The valid options based on the current configuration are {remaining-static-listener-names}
      """
    When the existing listener PLAINTEXT protocol is changed from PLAINTEXT to SSL
    Then the complete broker resource response has error 42 and message
      """
      Invalid value org.apache.kafka.common.config.ConfigException: Security protocol cannot be updated for existing listener ListenerName(PLAINTEXT) for configuration Invalid dynamic configuration
      """
    And both described listener settings retain their original static values
