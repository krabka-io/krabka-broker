Feature: Live streams membership locks group type across classic protocol and administration

  Scenario: streams group with live member rejects classic join
    Given a downgrade broker holds one-partition topic in2 for group g2
    When a streams member joins the downgrade group and converges on its single task
    Then the downgrade group has a healthy live Streams member
    And the downgrade group retains one live Streams member
    When these classic JoinGroup requests target the live streams group
      | member | instance   |
      | empty  | absent     |
      | m-1    | absent     |
      | empty  | instance-1 |
      | m-1    | instance-1 |
    Then all four classic joins return the complete INCONSISTENT_GROUP_PROTOCOL refusal
    And the downgrade group retains one live Streams member

  Scenario: converted group admin views respect type lock
    Given a downgrade broker holds one-partition topic in3 for group g3
    When a classic group is formed and its member leaves before streams conversion
    And a streams member joins the downgrade group and converges on its single task
    Then the downgrade group has a healthy live Streams member
    And the downgrade group retains one live Streams member
    When the classic admin client lists all groups after streams conversion
    Then the converted group is listed exactly once with streams type
    When the classic admin client attempts to delete the live converted streams group
    Then the live converted group refuses deletion with NON_EMPTY_GROUP and stays Streams-typed
