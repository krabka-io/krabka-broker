Feature: Deliberate loss injection uses records with unambiguous delivery history

  Scenario Outline: tests: only a record sent once and acknowledged is deleted
    Given an idempotent producer attempted 13 sequences, acknowledged 0 through 9, failed 5, and retried 6
    When the loss injector considers deleting sequence <sequence>
    Then the record is eligible for deliberate deletion: <eligible>

    Examples:
      | history                   | sequence | eligible |
      | acknowledged once         | 3        | true     |
      | never acknowledged        | 12       | false    |
      | failed then acknowledged  | 5        | false    |
      | resent                    | 6        | false    |
