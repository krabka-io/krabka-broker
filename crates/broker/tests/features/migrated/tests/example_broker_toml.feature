Feature: Deployable operator configuration examples

  Scenario: examples parse and apply as written
    Given the checked-in single-node and three-node broker examples
    When the examples are parsed as FileConfig
    And the example configuration is applied to broker defaults
    Then every example configures at least one listener

  Scenario: examples with a tls inter broker listener name its server
    Given the checked-in single-node and three-node broker examples
    When the examples are parsed as FileConfig
    And the example configuration is applied to broker defaults
    Then every TLS inter-broker listener has a non-default peer server name

  Scenario: examples parse with every disabled setting enabled
    Given the checked-in single-node and three-node broker examples
    When every disabled setting is enabled and parsed as FileConfig
    Then each example contains disabled settings and demonstrates operator keys

  Scenario: single node example covers the local surface
    Given the checked-in single-node broker example
    When the examples are parsed as FileConfig
    Then the single-node example covers local listeners and recovery features

  Scenario: three node example covers the cluster surface
    Given the checked-in three-node broker example
    When the examples are parsed as FileConfig
    And every disabled setting is enabled and parsed as FileConfig
    Then the three-node example covers quorum security and remote storage

  Scenario: every example key is in the schema
    Given the checked-in single-node and three-node broker examples
    When every disabled setting is enabled and parsed as TOML
    Then all enabled example keys are known by the configuration schema
