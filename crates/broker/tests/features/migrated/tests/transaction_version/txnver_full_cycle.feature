Feature: Downgraded transaction versions preserve complete transactional cycles

  Scenario Outline: versioned full cycles commit and read
    Given transaction.version is finalized at <level> with topic <topic> and producer <transactional_id>
    When the versioned producer commits a transaction containing a,b,c
    And a fresh read-committed group <group> consumes the versioned topic
    Then the versioned committed read contains exactly a,b,c

    Examples:
      | level | topic | transactional_id | group |
      | 1     | tv1   | tv1-tid          | tv1-g |
      | 0     | tv0   | tv0-tid          | tv0-g |
