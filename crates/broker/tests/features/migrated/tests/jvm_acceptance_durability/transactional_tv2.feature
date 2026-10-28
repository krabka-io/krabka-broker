Feature: tests jvm acceptance durability transactional tv2

  Scenario: a kafka 4x client runs transactions without add partitions to txn
    Then jvm acceptance durability transactional tv2: a kafka 4x client runs transactions without add partitions to txn

  Scenario: the transactions tool agrees with the broker and can abort
    Then jvm acceptance durability transactional tv2: the transactions tool agrees with the broker and can abort
