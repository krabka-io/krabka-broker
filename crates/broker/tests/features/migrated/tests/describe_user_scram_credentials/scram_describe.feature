Feature: tests describe user scram credentials scram describe

  Scenario: describe all users round trip
    Then describe user scram credentials scram describe: describe all users round trip

  Scenario: describe unknown user returns error
    Then describe user scram credentials scram describe: describe unknown user returns error

  Scenario: describe duplicate requested user returns single duplicate resource row
    Then describe user scram credentials scram describe: describe duplicate requested user returns single duplicate resource row
