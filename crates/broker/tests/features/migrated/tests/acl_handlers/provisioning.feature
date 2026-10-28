Feature: Provision and remove ACLs through an authenticated admin listener
  Scenario: create acls super user can provision and describe
    Given a super user admin on the SASL listener
    When admin creates a Read grant on foo for alice
    Then the Read grant creation returns one successful binding
    When admin describes all topic ACLs
    Then the only ACL is alice Read Allow on literal foo from any host

  Scenario: create acls non super user rejected
    Given alice is credentialed but only admin is a super user
    When alice creates Read on foo and Write on bar for other principals
    Then both requested bindings are denied with cluster authorization error 31

  Scenario: delete acls removes matching
    Given a super user admin on the SASL listener
    And admin has provisioned alice Read on foo and Write on bar
    When admin deletes only alice Read Allow on literal foo from any host
    Then the precise delete returns exactly the successful Read binding on foo
    When admin describes all topic ACLs
    Then only alice Write Allow on bar survives the targeted deletion
