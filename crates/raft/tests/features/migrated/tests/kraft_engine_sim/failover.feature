Feature: tests kraft engine sim failover

  Scenario: leader failure reelects
    Then kraft engine sim failover: leader failure reelects

  Scenario: repeated leader restart reelects
    Then kraft engine sim failover: repeated leader restart reelects

  Scenario: isolated leader resigns and rejoins after heal
    Then kraft engine sim failover: isolated leader resigns and rejoins after heal

  Scenario Outline: lockstep survivors elect within a fetch timeout and a few election timeouts
    Then kraft engine sim failover: lockstep survivors elect within a fetch timeout and a few election timeouts for <example>

    Examples:
      | example |
      | first   |
      | second  |
      | third   |
      | fourth  |
      | fifth   |
      | sixth   |
