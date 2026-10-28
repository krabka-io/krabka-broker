Feature: Offset deletion protects classic group subscriptions
  Scenario: An empty consumer group protects nothing
    Given a classic group is empty with protocol type consumer selected protocol absent and member metadata
      | version | topics |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects none
  Scenario: An empty Connect group protects nothing
    Given a classic group is empty with protocol type connect selected protocol absent and member metadata
      | version | topics |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects none
  Scenario: A stable Connect group refuses deletion
    Given a classic group is stable with protocol type connect selected protocol default and member metadata
      | version | topics |
      | opaque | ignored |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects nonempty-group-error
  Scenario: A preparing group without a protocol type refuses deletion
    Given a classic group is preparing with protocol type absent selected protocol absent and member metadata
      | version | topics |
      | empty | ignored |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects nonempty-group-error
  Scenario: Consumer subscriptions union across wire versions
    Given a classic group is stable with protocol type consumer selected protocol range and member metadata
      | version | topics |
      | 0 | a,b |
      | 3 | c |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects a,b,c
  Scenario: An undecodable member subscription protects every topic
    Given a classic group is completing with protocol type consumer selected protocol range and member metadata
      | version | topics |
      | 1 | a |
      | truncated | ignored |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects all
  Scenario: An unselected group protocol protects every topic
    Given a classic group is preparing with protocol type consumer selected protocol absent and member metadata
      | version | topics |
      | 0 | a |
    When the coordinator checks which topics a classic OffsetDelete must protect
    Then the deletion guard protects all
