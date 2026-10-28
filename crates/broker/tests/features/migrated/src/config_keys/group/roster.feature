Feature: Kafka trunk group configuration definitions are complete and ordered

  Scenario: Enumerate every group configuration exactly once in parse order
    Given Kafka trunk group configuration definitions
    When the group key names and definition parse order are listed
    Then the roster has 27 alphabetically ordered names and parse order contains every name once
