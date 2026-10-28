Feature: tests share admin offsets alter

  Scenario: alter resets empty group
    Then share admin offsets alter: alter resets empty group

  Scenario: alter non empty group fenced
    Then share admin offsets alter: alter non empty group fenced
