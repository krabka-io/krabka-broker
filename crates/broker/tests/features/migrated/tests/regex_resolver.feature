Feature: Consumers supply custom regular expression resolution

  Scenario: a consumer can implement and call a custom resolver
    Given a custom orders resolver subscription matching orders.*
    When a consumer resolves the subscriptions through the public resolver trait
    Then orders resolves to topic orders at version 42 and timestamp 1000 milliseconds
