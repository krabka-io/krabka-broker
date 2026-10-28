Feature: tests transactions txn uncommitted writes

  Scenario: an end txn is answered only once its commit decision is replicated
    Then transactions txn uncommitted writes: an end txn is answered only once its commit decision is replicated

  Scenario: a transaction completes only once its markers are committed
    Then transactions txn uncommitted writes: a transaction completes only once its markers are committed

  Scenario: group coordinator writes are answered only once committed
    Then transactions txn uncommitted writes: group coordinator writes are answered only once committed
