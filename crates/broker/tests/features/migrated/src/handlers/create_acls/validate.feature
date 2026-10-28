Feature: src handlers create acls validate

  Scenario: tests: validate accepts any principal type and the kafka cluster name
    Then handlers create acls validate tests: validate accepts any principal type and the kafka cluster name

  Scenario: tests: validate refuses with kafka messages
    Then handlers create acls validate tests: validate refuses with kafka messages

  Scenario: tests: whole request element checks match kafka parse and binding construction
    Then handlers create acls validate tests: whole request element checks match kafka parse and binding construction

  Scenario: tests: validate host matches kafka cidr gating
    Then handlers create acls validate tests: validate host matches kafka cidr gating
