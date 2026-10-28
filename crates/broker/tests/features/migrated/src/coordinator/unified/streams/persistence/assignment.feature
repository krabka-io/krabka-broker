Feature: src coordinator unified streams persistence assignment

  Scenario: tests: target assignment member bytes match kafka schema
    Then coordinator unified streams persistence assignment tests: target assignment member bytes match kafka schema

  Scenario: tests: current member assignment bytes match kafka schema
    Then coordinator unified streams persistence assignment tests: current member assignment bytes match kafka schema

  Scenario: tests: current member assignment epochs match kafka schema
    Then coordinator unified streams persistence assignment tests: current member assignment epochs match kafka schema

  Scenario: tests: wire state matches kafkas stream member discriminants
    Then coordinator unified streams persistence assignment tests: wire state matches kafkas stream member discriminants

  Scenario Outline: tests: wire state matches kafkas streams member state
    Then coordinator unified streams persistence assignment tests: wire state matches kafkas streams member state for <example>

    Examples:
      | example          |
      | stable           |
      | unrevoked tasks  |
      | unreleased tasks |
