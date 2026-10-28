Feature: tests acl handlers metadata group txn

  Scenario: metadata silent filter on fetch all
    Then acl handlers metadata group txn: metadata silent filter on fetch all

  Scenario: metadata explicit deny on named topic
    Then acl handlers metadata group txn: metadata explicit deny on named topic

  Scenario: join group denied without group read acl
    Then acl handlers metadata group txn: join group denied without group read acl

  Scenario: init producer id denied without txn acl
    Then acl handlers metadata group txn: init producer id denied without txn acl
