Feature: tests jvm acceptance sasl acl denied

  Scenario: jvm unauthorized produce fails
    Then jvm acceptance sasl acl denied: jvm unauthorized produce fails

  Scenario: jvm unauthorized consumer fails group check
    Then jvm acceptance sasl acl denied: jvm unauthorized consumer fails group check
