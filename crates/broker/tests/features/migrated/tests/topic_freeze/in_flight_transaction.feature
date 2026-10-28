Feature: A freeze permits completion of transactions already enlisted
  Scenario: a transaction that enlisted before the freeze still commits
    Given an open transaction has acknowledged one orders record before a freeze
    When a committed reader asks for the end of the open orders transaction
    Then the open transaction pins the last stable offset at zero
    When orders is frozen for cutover while its transaction remains open
    And a new plain write attempts to enter frozen orders
    Then the new write is refused without advancing orders beyond its enlisted record
    When the transaction enlisted before the freeze commits
    Then the commit marker advances both the orders log and stable offset to two
    When the enlisted-transaction control topic receives a record
    Then the enlisted-transaction control write is accepted at log end one
