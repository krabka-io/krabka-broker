Feature: Cluster Describe permission authorizes credential reads without a super user bypass
  Scenario: describe allows cluster describe acl
    Given alice authenticates with her test password and no principal is a super user
    And alice has cluster Describe permission
    When the authenticated principal describes SCRAM users all
    Then the SCRAM description has top level error 0
    And the SCRAM description has no per-user rows

  Scenario: describe rejects without cluster describe acl
    Given alice authenticates with a fresh UUID password and no principal is a super user
    When the authenticated principal describes SCRAM users all
    Then the SCRAM description has top level error 31
    And the SCRAM description has no per-user rows
