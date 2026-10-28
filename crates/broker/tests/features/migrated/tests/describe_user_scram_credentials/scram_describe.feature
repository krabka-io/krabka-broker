Feature: Describe SCRAM credentials reports real seeded unknown and duplicate user rows
  Scenario: describe all users round trip
    Given admin is a super user authenticated with its test password
    And user alice has a SHA 512 SCRAM credential at 4096 iterations
    When the authenticated principal describes SCRAM users all
    Then the SCRAM description has top level error 0
    And alice has a successful credential row containing mechanism 2

  Scenario: describe unknown user returns error
    Given admin is a super user authenticated with its test password
    When the authenticated principal describes SCRAM users ghost
    Then the SCRAM description has top level error 0
    And ghost has resource not found error 91

  Scenario: describe duplicate requested user returns single duplicate resource row
    Given admin is a super user authenticated with a timestamped password
    And user alice has a SHA 512 SCRAM credential at 4096 iterations
    And user bob has a SHA 512 SCRAM credential at 8192 iterations
    When the authenticated principal describes SCRAM users alice,bob,alice
    Then the SCRAM description has top level error 0
    And duplicate names collapse to these exact per-user results:
      | user | error | credentials |
      | alice | 92 | none |
      | bob | 0 | 2:8192 |
