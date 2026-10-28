Feature: Producer inspection tracks transaction boundaries and coordinator epochs
  Scenario: transactional fields follow open and completed transactions
    Given a standalone broker is ready to inspect producer state
    And producer topic transactions exists with one partition
    And a transactional producer describe-producers-tid is initialized for transactions
    When the transactional producer appends first at base sequence 0
    Then the transactional append succeeds
    When the client describes producers for topic transactions partitions 0
    Then the active producer reports transaction start 0 and coordinator epoch -1
    When a version 1 transaction marker completes the transaction at coordinator epoch 17
    Then the transaction marker succeeds
    When the client describes producers for topic transactions partitions 0
    Then the active producer reports transaction start -1 and coordinator epoch 17
    When the transactional producer appends second at base sequence 1
    Then the transactional append succeeds
    When the client describes producers for topic transactions partitions 0
    Then the active producer reports transaction start 2 and coordinator epoch 17
