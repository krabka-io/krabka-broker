Feature: tests jvm acceptance sasl acl cli

  Scenario: jvm kafka acls provision via cli
    Then jvm acceptance sasl acl cli: jvm kafka acls provision via cli

  Scenario: acl shorthands expand as apache kafka expands them
    Then jvm acceptance sasl acl cli: acl shorthands expand as apache kafka expands them
