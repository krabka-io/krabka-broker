Feature: A new producer epoch starts with sequence zero before and after log recovery

  Scenario Outline: first sequence at a new epoch gets the same answer before and after restart
    Given a producer has advanced its epoch through <setup>
    When the producer sends sequence 3 at the new epoch before restart
    Then the latest epoch probe returns OUT_OF_ORDER_SEQUENCE_NUMBER
    When the broker restarts on the same data directory
    And the producer sends sequence 3 at the same new epoch after restart
    Then the latest epoch probe returns OUT_OF_ORDER_SEQUENCE_NUMBER
    When the producer sends sequence 0 at the new epoch after restart
    Then the complete live and recovered sequence results are 45,45,0

    Examples:
      | setup                              |
      | a TV2 transaction commit           |
      | a TV1 cluster EndTxn-v5 commit      |
      | an idempotent local epoch bump     |
