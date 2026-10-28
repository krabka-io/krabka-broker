Feature: Operator-managed client metrics subscriptions

  Scenario: client metrics config alter describe list round trip
    Given a single-broker cluster accepting client metrics subscriptions
    And subscription sub-a selects consumer metrics every 60000 milliseconds
    When the operator creates subscription sub-a with IncrementalAlterConfigs
    Then the subscription change succeeds for exactly one resource
    When the operator describes subscription sub-a
    Then the description preserves metrics and interval with client-metrics sources and the default match
    When the operator lists client metrics configuration resources
    Then subscription sub-a appears in the client metrics resource list
