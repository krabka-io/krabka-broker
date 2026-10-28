Feature: Broker consumes formatter bootstrap credentials and rejects corrupt bootstrap data
  Scenario: bootstrap records provisions scram user
    Given a formatted bootstrap log seeds alice wonderland with SCRAM SHA 512 at 4096 iterations
    When the broker consumes its configured bootstrap log at startup
    Then the broker starts successfully
    When alice authenticates through SCRAM and requests Metadata
    Then the seeded credential authenticates and serves Metadata immediately after startup

  Scenario: corrupt bootstrap refuses start
    Given bootstrap records contain bytes that are not a length prefixed metadata record
    When the broker consumes its configured bootstrap log at startup
    Then startup is refused specifically with a BootstrapFile error

  Scenario: bootstrap absent legacy path
    Given a fresh log directory has no bootstrap records file
    When the broker consumes its configured bootstrap log at startup
    Then the broker starts successfully
