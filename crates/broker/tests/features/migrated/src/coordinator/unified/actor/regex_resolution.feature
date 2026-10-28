Feature: Consumer regex resolution refresh timing and pattern changes
  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (1: age 60000, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 60000 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 5
    Then the pattern update is unchanged and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (2: age 60000, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 60000 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 6
    Then the pattern update is unchanged and the resolver is invoked true

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (3: age 10000, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 10000 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 6
    Then the pattern update is unchanged and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (4: age 10001, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 10001 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 6
    Then the pattern update is unchanged and the resolver is invoked true

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (5: age 600001, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 600001 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 5
    Then the pattern update is unchanged and the resolver is invoked true

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (6: age 600000, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 600000 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern a.* against metadata version 5
    Then the pattern update is unchanged and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (7: age 5000, old absent)
    Given m1 uses a.* resolved at metadata version 5 with age 5000 milliseconds and m2 previously uses absent
    When m2 heartbeats with pattern b.* against metadata version 5
    Then the pattern update is updated and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (8: age 60000, old absent)
    Given m1 uses a.* resolved at metadata version 5 with age 60000 milliseconds and m2 previously uses absent
    When m2 heartbeats with pattern b.* against metadata version 5
    Then the pattern update is updated and the resolver is invoked true

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (9: age 60000, old absent)
    Given m1 uses a.* resolved at metadata version 5 with age 60000 milliseconds and m2 previously uses absent
    When m2 heartbeats with pattern a.* against metadata version 5
    Then the pattern update is updated-and-resolved and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (10: age 600001, old absent)
    Given m1 uses a.* resolved at metadata version 5 with age 600001 milliseconds and m2 previously uses absent
    When m2 heartbeats with pattern absent against metadata version 6
    Then the pattern update is unchanged and the resolver is invoked true

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (11: age 5000, old absent)
    Given m1 uses a.* resolved at metadata version 5 with age 5000 milliseconds and m2 previously uses absent
    When m2 heartbeats with pattern absent against metadata version 6
    Then the pattern update is unchanged and the resolver is invoked false

  Scenario: Kafka refresh intervals and already shared resolutions govern regex refreshes (12: age 60000, old a.*)
    Given m1 uses a.* resolved at metadata version 5 with age 60000 milliseconds and m2 previously uses a.*
    When m2 heartbeats with pattern absent against metadata version 5
    Then the pattern update is updated-and-resolved and the resolver is invoked false
