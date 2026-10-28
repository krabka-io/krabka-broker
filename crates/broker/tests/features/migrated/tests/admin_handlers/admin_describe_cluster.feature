Feature: Discovering the broker cluster through the correct endpoint

  Scenario: describe cluster lists brokers
    Given an admin client connected to one standalone broker
    When the admin requests the broker cluster projection
    Then DescribeCluster succeeds with broker 1 as the only broker and controller

  Scenario: describe cluster endpoint type controllers is rejected
    Given an admin client connected to one standalone broker
    When the admin requests a controller projection from the broker endpoint
    Then DescribeCluster reports MISMATCHED_ENDPOINT_TYPE with an empty broker projection
