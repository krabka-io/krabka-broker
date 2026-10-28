Feature: tests describe producers producers errors

  Scenario: unknown topic returns unknown topic or partition
    Then describe producers producers errors: unknown topic returns unknown topic or partition

  Scenario: out of range partition returns unknown topic or partition
    Then describe producers producers errors: out of range partition returns unknown topic or partition

  Scenario: metadata known partition not hosted locally returns not leader
    Then describe producers producers errors: metadata known partition not hosted locally returns not leader
