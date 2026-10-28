Feature: tests alter replica log dirs errors

  Scenario: alter replica log dirs rejects unknown target
    Then alter replica log dirs errors: alter replica log dirs rejects unknown target

  Scenario: alter replica log dirs rejects unknown replica
    Then alter replica log dirs errors: alter replica log dirs rejects unknown replica

  Scenario: alter replica log dirs denied without cluster alter
    Then alter replica log dirs errors: alter replica log dirs denied without cluster alter
