Feature: tests describe user scram credentials scram authorization

  Scenario: describe allows cluster describe acl
    Then describe user scram credentials scram authorization: describe allows cluster describe acl

  Scenario: describe rejects without cluster describe acl
    Then describe user scram credentials scram authorization: describe rejects without cluster describe acl
