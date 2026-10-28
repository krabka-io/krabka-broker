Feature: Finalized group version gates the next generation consumer protocol
  Scenario: next gen accepted when group version finalized
    Given a freshly bootstrapped broker finalizing group version 1
    When a next generation member sends a joining heartbeat to group gv-accept
    Then the heartbeat passes the group version feature gate

  Scenario: next gen rejected when group version disabled
    Given a freshly bootstrapped broker finalizing group version 1
    When the admin safely downgrades group version to 0
    Then the group version downgrade succeeds
    When a next generation member sends a joining heartbeat to group gv-reject
    Then the heartbeat is rejected with UNSUPPORTED_VERSION error 35
