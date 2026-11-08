Feature: The runner executes scenarios built by hand

  Scenario: Every step passes
    Given the runner counter starts at 2
    * the runner counter is doubled after a pause
    Then the runner counter is 4

  Rule: A failing step names where it failed

    Scenario Outline: A step fails
      Given the runner counter starts at <start>
      Then the runner counter is <expected>

      Examples:
        | case     | start | expected |
        | mismatch | 1     | 5        |

  Scenario: An attempt reaches a later Then
    When the runner attempts to parse x
    Then the runner attempt failed with invalid digit

  Scenario: A step matches two definitions equally well
    Given the runner flips heads coins

  Scenario: A step matches no definition
    Given no definition matches this runner step

  @allow_skipped
  Scenario: A step skips the rest of the scenario
    Given the runner skips because no broker is running
    Then the runner fails if it is reached

  @inprogress
  Scenario: A scenario in progress
    Given the runner counter starts at 1
    Then the runner counter is 4
