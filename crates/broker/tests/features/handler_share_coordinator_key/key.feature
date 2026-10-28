Feature: Share coordinator keys preserve colons in group identifiers
  Scenario: The final UUID and partition delimit the group name
    Given the share coordinator key is group:with:colon:BQUFBQUFBQUFBQUFBQUFBQ:7
    When the share coordinator key is parsed
    Then the coordinator group is group:with:colon topic bytes are all 5 and partition is 7
