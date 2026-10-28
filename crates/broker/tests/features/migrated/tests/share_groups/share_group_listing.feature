Feature: Group listings distinguish share groups from consumer groups

  Scenario Outline: list groups includes share group
    Given share group g7 has joined two-partition topic t7
    When ListGroups uses the <filter> group-type filter
    Then g7 is present with group type share: <present>

    Examples:
      | filter   | present |
      | share    | true    |
      | consumer | false   |
      | all      | true    |
