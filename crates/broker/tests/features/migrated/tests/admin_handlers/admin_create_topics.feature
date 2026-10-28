Feature: Effective configuration returned when a topic is created

  Scenario: create topics returns the configs describe configs reports
    Given an admin client connected to one standalone broker
    And topic t-kip525 requests retention.ms 60000
    When the admin creates the topic with one partition and one replica
    Then the topic is created with its effective retention override in the response
    When the admin describes the created topic configuration
    Then DescribeConfigs succeeds and returns exactly the effective create configuration
