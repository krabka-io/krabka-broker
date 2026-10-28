Feature: tests jvm acceptance sasl acl security disabled

  Scenario: acl list is refused as apache kafka refuses it with no authorizer
    Then jvm acceptance sasl acl security disabled: acl list is refused as apache kafka refuses it with no authorizer
