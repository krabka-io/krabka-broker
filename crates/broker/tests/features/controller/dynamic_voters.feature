Feature: Dynamic voter membership grows through auto-join and shrinks through leader-aware removal

  Scenario: Auto-join grows a standalone quorum to three
    Given node 1 bootstraps alone with kraft.version 1 and a 500ms controller fetch timeout
    When nodes 2 and 3 join using AddRaftVoter auto-join over the controller listener
    Then the leader and every joined node commit exactly voter IDs 1, 2 and 3

  Scenario Outline: Removing either controller role shrinks the committed quorum
    Given a three-voter dynamic quorum selects its <role> for removal by its committed directory ID
    When the admin removes that voter starting at a follower and follows controller leadership changes
    Then every node commits the remaining two-voter set, the removed node follows as an observer and a remaining voter leads

    Examples:
      | role |
      | follower |
      | leader |
