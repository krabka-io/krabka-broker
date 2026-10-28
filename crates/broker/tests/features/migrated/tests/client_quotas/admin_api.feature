Feature: Authorized quota mutations become visible in committed state and descriptions
  Background:
    Given admin is a super user and alice is an ordinary authenticated principal

  Scenario: alter then describe round trip
    When admin sets alice producer byte rate to 1024
    Then the admin quota alteration returns one successful entry
    And alice committed producer byte rate is 1024
    When admin describes quotas for any named user
    Then DescribeClientQuotas includes alice producer byte rate 1024

  Scenario: non super user denied
    Given the empty ACL compatibility shortcut is disabled
    When alice attempts to set her producer byte rate to 999
    Then the requested quota entry is refused with cluster authorization error 31
