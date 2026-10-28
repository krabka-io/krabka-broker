Feature: tests transactions txn fencing

  Scenario: fenced producer cannot commit
    Then transactions txn fencing: fenced producer cannot commit

  Scenario: init producer id fences a stale producer identity
    Then transactions txn fencing: init producer id fences a stale producer identity

  Scenario: txn offset commit fences classic generation and member
    Then transactions txn fencing: txn offset commit fences classic generation and member

  Scenario: txn offset commit fences next gen member epoch
    Then transactions txn fencing: txn offset commit fences next gen member epoch
