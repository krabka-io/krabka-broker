Feature: tests share consume isolation level

  Scenario: read committed skips open txn then sees committed
    Then share consume isolation level: read committed skips open txn then sees committed

  Scenario: aborted transaction data is archived under read committed
    Then share consume isolation level: aborted transaction data is archived under read committed
