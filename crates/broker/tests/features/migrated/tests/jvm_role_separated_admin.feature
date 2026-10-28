Feature: Forwarded administration in a role-separated cluster
  Scenario: Broker-only nodes forward configuration and feature administration
    Given one controller-only voter and two registered broker-only metadata observers
    When the JVM administrator creates single-replica topic role-separated-admin through a broker-only node
    Then every forwarded administration command succeeds
    When the administrator alters topic configuration to retention.ms=60000 through a broker-only node
    Then every forwarded administration command succeeds
    And the tool and both broker-only images report retention.ms=60000
    When the administrator requests downgrade of feature transaction.version to 1 through a broker-only node
    Then every forwarded administration command succeeds
    And the feature tool reports finalized transaction.version level 1
    When the administrator requests upgrade of feature transaction.version to 2 through a broker-only node
    Then every forwarded administration command succeeds
    And the feature tool reports finalized transaction.version level 2
