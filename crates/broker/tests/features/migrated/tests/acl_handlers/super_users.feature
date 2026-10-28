Feature: Every configured super user can provision ACLs
  Scenario: multi super user both can provision
    Given admin and ops-bot are super users while credentialed alice has no cluster grants
    When admin provisions a topic binding
    Then all 1 ACL binding results have error code 0
    When ops-bot provisions a topic binding
    Then all 1 ACL binding results have error code 0
    When alice provisions two topic bindings
    Then all 2 ACL binding results have error code 31
