Feature: Transaction state codecs recover ongoing transactions with their producer identities

  Scenario Outline: versioned ongoing transactions survive restart and decode recovery
    Given an ongoing transaction <transactional_id> on topic <topic> persisted at transaction version <level>
    When the transaction broker restarts and decodes the same durable state directory
    And the original producer commits the recovered ongoing transaction
    Then the recovered EndTxn response succeeds with the original producer ID and its next epoch

    Examples:
      | transactional_id | topic | level |
      | recover-v1-tid   | rec1  | 2     |
      | recover-v0-tid   | rec0  | 0     |
