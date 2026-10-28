Feature: tests transactions 2pc

  Scenario: enable 2pc rejected when cluster disabled
    Then transactions 2pc: enable 2pc rejected when cluster disabled

  Scenario: keep prepared txn without ongoing transaction is a noop
    Then transactions 2pc: keep prepared txn without ongoing transaction is a noop

  Scenario: enable 2pc persists no timeout sentinel
    Then transactions 2pc: enable 2pc persists no timeout sentinel

  Scenario: two phase commit needs no transaction version 3 and 3 is refused
    Then transactions 2pc: two phase commit needs no transaction version 3 and 3 is refused
