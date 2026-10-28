Feature: tests example broker toml

  Scenario: examples parse and apply as written
    Then example broker toml: examples parse and apply as written

  Scenario: examples with a tls inter broker listener name its server
    Then example broker toml: examples with a tls inter broker listener name its server

  Scenario: examples parse with every disabled setting enabled
    Then example broker toml: examples parse with every disabled setting enabled

  Scenario: single node example covers the local surface
    Then example broker toml: single node example covers the local surface

  Scenario: three node example covers the cluster surface
    Then example broker toml: three node example covers the cluster surface

  Scenario: every example key is in the schema
    Then example broker toml: every example key is in the schema
