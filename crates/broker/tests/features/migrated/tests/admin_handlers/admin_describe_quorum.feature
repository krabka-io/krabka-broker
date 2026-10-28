Feature: tests admin handlers admin describe quorum

  Scenario: describe quorum reports cluster metadata voter set
    Then admin handlers admin describe quorum: describe quorum reports cluster metadata voter set

  Scenario: combined follower forwards describe quorum to the active controller
    Then admin handlers admin describe quorum: combined follower forwards describe quorum to the active controller

  Scenario: describe quorum for another topic is unknown topic or partition
    Then admin handlers admin describe quorum: describe quorum for another topic is unknown topic or partition
