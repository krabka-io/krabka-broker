Feature: Discovering administrative resources

  Scenario: list config resources default set includes topics and brokers
    Given an admin client connected to one standalone broker
    And topics t-lcr-a and t-lcr-b each have one partition
    When the admin lists configuration resources without a type filter
    Then the resource list succeeds and includes both topics and exactly broker 1

  Scenario: list groups includes freshly created group
    Given an admin client connected to one standalone broker
    And the broker already knows consumer group test-group-listed
    When the admin lists consumer groups
    Then the group list succeeds and contains test-group-listed
