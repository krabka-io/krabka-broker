Feature: Operating the broker executable

  Scenario: help mentions cluster id and advertised listener
    Given the built broker executable
    When the broker is invoked with --help
    Then the broker command exits successfully
    And the help lists --cluster-id and --advertised-listener

  Scenario: version returns zero
    Given the built broker executable
    When the broker is invoked with --version
    Then the broker command exits successfully

  Scenario: boots with config file listener
    Given the built broker executable
    And a config file with one PLAIN listener bound and advertised on port zero
    When the broker boots using the config-file listener
    Then the startup listener has a nonzero port accepting TCP connections

  Scenario: errors when config file and listen addr both set
    Given the built broker executable
    When the broker is invoked with both --config-file and --listen-addr
    Then the command fails with a config-file and listen-addr mutual-exclusion error

  Scenario: refuses a log dir formatted for another node
    Given the built broker executable
    And a standalone log directory formatted for node 1
    When node 2 attempts to boot from that directory
    Then the broker exits 1 with the stored-node-id mismatch and recovery guidance

  Scenario: each role opens only its own listeners
    Given controller-only, broker-only joining that quorum, and standalone combined nodes
    When the role-configured nodes boot and both their listener ports are probed
    Then each startup line and port expose only the listeners required by its roles
