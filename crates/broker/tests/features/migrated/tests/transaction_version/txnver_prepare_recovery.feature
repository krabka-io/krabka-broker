Feature: tests transaction version txnver prepare recovery

  Scenario: end txn cut between prepare and complete completes after restart
    Then transaction version txnver prepare recovery: end txn cut between prepare and complete completes after restart

  Scenario: init producer id during prepare commit is concurrent transactions
    Then transaction version txnver prepare recovery: init producer id during prepare commit is concurrent transactions

  Scenario: end txn with a failed marker fanout answers none and completes later
    Then transaction version txnver prepare recovery: end txn with a failed marker fanout answers none and completes later
