Feature: tests authorized operations

  Scenario: metadata topic authorized operations default is not present
    Then authorized operations: metadata topic authorized operations default is not present

  Scenario: metadata topic authorized operations super user gets full mask
    Then authorized operations: metadata topic authorized operations super user gets full mask

  Scenario: describe cluster authorized operations default is not present
    Then authorized operations: describe cluster authorized operations default is not present

  Scenario: describe cluster authorized operations super user gets full mask
    Then authorized operations: describe cluster authorized operations super user gets full mask

  Scenario: describe groups authorized operations default is not present
    Then authorized operations: describe groups authorized operations default is not present

  Scenario: describe groups authorized operations super user gets full mask
    Then authorized operations: describe groups authorized operations super user gets full mask

  Scenario: metadata cluster authorized operations super user gets full mask v9
    Then authorized operations: metadata cluster authorized operations super user gets full mask v9
