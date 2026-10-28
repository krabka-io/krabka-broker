Feature: Reading the authoritative controller quorum through broker listeners

  Scenario: describe quorum reports cluster metadata voter set
    Given an admin client connected to one standalone broker
    When the admin describes the cluster metadata quorum
    Then the metadata quorum names broker 1 as its only voter and elected leader

  Scenario: combined follower forwards describe quorum to the active controller
    Given an admin client connected to two combined brokers
    And the active controller of the two-broker quorum is known
    When the non-controller broker receives DescribeQuorum
    Then the forwarded quorum response matches the active controller with real voter progress

  Scenario: describe quorum for another topic is unknown topic or partition
    Given an admin client connected to two combined brokers
    When the admin asks DescribeQuorum for a non-metadata topic
    Then the unknown quorum partition returns Kafka UNKNOWN_TOPIC_OR_PARTITION and its message
