Feature: Controller bootstrap routes supported APIs and rejects broker-only APIs locally
  Scenario: controller bootstrap routes supported and rejects unsupported admin rpc
    Given a broker serves separate data and controller endpoints
    And topic controller-admin already exists on the broker
    When AdminClient connects using the controller bootstrap endpoint
    And the controller admin describes controller-admin topic configuration
    Then the controller returns only controller-admin configuration successfully
    When the controller admin attempts replication factor reconciliation
    Then reconciliation is locally refused with ControllerEndpoint error 115 UNSUPPORTED_ENDPOINT_TYPE
    When the controller admin creates created-through-controller
    Then the topic creation through the controller succeeds
    When the controller admin requests alice user quotas
    Then quota discovery is locally refused with ControllerEndpoint error 115 UNSUPPORTED_ENDPOINT_TYPE
    When the controller admin describes controller-admin topic configuration
    Then the controller returns only controller-admin configuration successfully

  Scenario: controller bootstrap rejects broker endpoint
    Given a broker serves separate data and controller endpoints
    When AdminClient treats the data endpoint as a controller bootstrap endpoint
    Then bootstrap is refused with DescribeCluster error 114 MISMATCHED_ENDPOINT_TYPE
