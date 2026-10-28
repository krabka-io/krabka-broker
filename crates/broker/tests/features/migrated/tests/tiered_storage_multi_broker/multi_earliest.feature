Feature: tests tiered storage multi broker multi earliest

  Scenario: a tiered partition reads back from earliest after its leader restarts
    Then tiered storage multi broker multi earliest: a tiered partition reads back from earliest after its leader restarts

  Scenario: a follower left with a raised floor takes its leaders back
    Then tiered storage multi broker multi earliest: a follower left with a raised floor takes its leaders back

  Scenario: a compressed archive reads back from earliest
    Then tiered storage multi broker multi earliest: a compressed archive reads back from earliest
