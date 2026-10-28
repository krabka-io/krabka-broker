Feature: Bootstrap controller discovery follows dynamic quorum membership

  Scenario: Bootstrap discovery survives adding an observer and removing the original leader
    Given standalone controller 3001 leads alone, controller 3002's bootstrap port refuses connections and broker nodes 1 and 2 have only both bootstrap controller endpoints
    When brokers 1 and 2 start using controller bootstrap discovery without a local voter set
    Then DescribeQuorum through broker 1 names leader 3001, voters 3001 and observers 1,2
    When controller 3002 starts with no initial controllers and discovers the standalone quorum as an observer
    Then DescribeQuorum through broker 1 names leader 3001, voters 3001 and observers 1,2,3002
    When the admin adds controller 3002 as a voter through bootstrap controller 3001
    Then DescribeQuorum through broker 1 names leader 3001, voters 3001,3002 and observers 1,2
    When the admin removes current leader 3001 using both controller bootstrap endpoints
    Then DescribeQuorum through broker 1 names leader 3002, voters 3002 and observers 1,2,3001
    And neither broker is fenced after a full heartbeat session under controller 3002

  Scenario: Wildcard controller bindings advertise a reachable hostname
    Given standalone controller 3001 is formatted with a loopback voter endpoint but binds its controller listener to 0.0.0.0
    When the wildcard-bound controller starts and elects itself
    Then the voter set advertises this machine's hostname on the bound controller port with listener name CONTROLLER
