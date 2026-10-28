Feature: Soak cluster authentication and host connectivity

  Scenario Outline: tests: the token names the principal a wal leader maps to the node
    Given an inter-broker token issued for node <node_id>
    When the WAL leader validates that token at 1800000000000 milliseconds
    Then the authenticated principal is <principal>

    Examples:
      | node_id | principal |
      | 1       | broker-1  |
      | 3       | broker-3  |

  Scenario: tests: the host gateway name must reach loopback
    Given the host gateway lookup returns addresses 127.0.0.1:0
    When the soak runner checks host gateway connectivity before starting containers
    Then the gateway configuration is accepted

  Scenario: a loopback address among other addresses is sufficient
    Given the host gateway lookup returns addresses 10.0.0.5:0,[::1]:0
    When the soak runner checks host gateway connectivity before starting containers
    Then the gateway configuration is accepted

  Scenario: an unresolved host gateway explains how to configure loopback
    Given the host gateway lookup fails to resolve
    When the soak runner checks host gateway connectivity before starting containers
    Then the gateway refusal explains does not resolve on this host and includes the loopback hosts entry

  Scenario: a non-loopback host gateway explains how to configure loopback
    Given the host gateway lookup returns addresses 10.0.0.5:0
    When the soak runner checks host gateway connectivity before starting containers
    Then the gateway refusal explains not to loopback and includes the loopback hosts entry
