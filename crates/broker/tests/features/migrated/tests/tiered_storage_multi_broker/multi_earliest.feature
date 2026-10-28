Feature: Earliest reads retain the global archive floor across restart and compression

  Scenario: a tiered partition reads back from earliest after its leader restarts
    Given an RF3 tiered partition led by broker 2 archives 120 records of 200 bytes using none and broker 2 evicts offset zero locally
    When broker 2 stops, broker 3 takes leadership and broker 2 restarts from its retained local log
    And preferred election returns leadership to broker 2
    And twenty more records at offsets 120 through 139 are acknowledged and replicated everywhere
    And broker 2 stops again and broker 3 takes leadership
    And fresh consumers seek to earliest and read toward offset 140 at both isolation levels
    Then each consumer returns every offset from 0 through 139 exactly once

  Scenario: a follower left with a raised floor takes its leaders back
    Given an RF3 tiered partition led by broker 2 archives 120 records of 200 bytes using none and broker 3 evicts offset zero locally
    When broker 3 stops and its persisted global floor is raised to its first remaining local segment
    And broker 3 restarts and follows broker 2
    And twenty more records at offsets 120 through 139 are acknowledged and replicated everywhere
    Then broker 3 lowers its global log start back to 0
    When broker 2 stops again and broker 3 takes leadership
    And fresh consumers seek to earliest and read toward offset 140 at both isolation levels
    Then each consumer returns every offset from 0 through 139 exactly once

  Scenario: a compressed archive reads back from earliest
    Given an RF3 tiered partition led by broker 2 archives 120 records of 200 bytes using lz4 and broker 2 evicts offset zero locally
    When fresh consumers seek to earliest and read toward offset 120 at both isolation levels
    Then each consumer returns every offset from 0 through 119 exactly once
