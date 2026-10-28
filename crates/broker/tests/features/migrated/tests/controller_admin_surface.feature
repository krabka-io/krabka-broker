Feature: tests controller admin surface

  Scenario: controller api versions advertises the kafka controller admin surface
    Then controller admin surface: controller api versions advertises the kafka controller admin surface

  Scenario: controller listener advertises no key kafka does not
    Then controller admin surface: controller listener advertises no key kafka does not

  Scenario: controller listener serves the topic lifecycle
    Then controller admin surface: controller listener serves the topic lifecycle

  Scenario: controller listener serves the writing delegation token apis
    Then controller admin surface: controller listener serves the writing delegation token apis

  Scenario: controller listener serves the scram write path
    Then controller admin surface: controller listener serves the scram write path

  Scenario: controller listener serves assign replicas to dirs
    Then controller admin surface: controller listener serves assign replicas to dirs

  Scenario: controller listener serves unregister controller
    Then controller admin surface: controller listener serves unregister controller

  Scenario: controller only node places no replica on itself
    Then controller admin surface: controller only node places no replica on itself

  Scenario: controller listener registers a restarted broker as kafka does
    Then controller admin surface: controller listener registers a restarted broker as kafka does

  Scenario: controller listener serves the inter broker and sasl apis
    Then controller admin surface: controller listener serves the inter broker and sasl apis
