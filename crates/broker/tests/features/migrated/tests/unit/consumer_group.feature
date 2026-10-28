Feature: tests unit consumer group

  Scenario: join group with empty member returns member id required
    Then unit consumer group: join group with empty member returns member id required

  Scenario: join group single member completes after deadline
    Then unit consumer group: join group single member completes after deadline

  Scenario: full group flow join sync heartbeat commit fetch leave
    Then unit consumer group: full group flow join sync heartbeat commit fetch leave
