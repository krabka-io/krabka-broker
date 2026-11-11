Feature: Topic authorization batches preserve request identity

  Rule: Repeated topic checks keep request identity across denial logging policies

    Scenario Outline: Repeated topic names retain each authorization check
      Given an authorizer denies WRITE access for principal batch from 127.0.0.1:9092 to these topics:
        | topic  |
        | first  |
        | first  |
        | second |
      When the broker checks the batch with <mode> denial logging
      Then the authorization checks are recorded in request order:
        | topic  | logged   |
        | first  | <logged> |
        | first  | <logged> |
        | second | <logged> |
      And the authorization results deny only these topics:
        | topic  |
        | first  |
        | second |
      And the results reuse the caller's topic names

      Examples:
        | case                    | mode     | logged |
        | default denial logging  | default  | true   |
        | explicit denial logging | enabled  | true   |
        | quiet authorization     | disabled | false  |
