Feature: Teardown runs however a scenario ends

  @teardown
  Scenario: The scenario passes
    Given the teardown world is ready

  @teardown
  Scenario: A step fails
    Given the teardown world is ready
    When the teardown step fails

  @teardown
  Scenario: A step panics
    Given the teardown world is ready
    When the teardown step panics

  @teardown @allow_skipped
  Scenario: A step skips
    Given the teardown world is ready
    When the teardown step skips
