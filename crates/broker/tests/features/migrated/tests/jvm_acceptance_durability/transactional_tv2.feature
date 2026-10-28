Feature: Kafka 4 transaction version 2 interoperability
  Scenario: a kafka 4x client runs transactions without add partitions to txn
    Given a transaction.version 2 broker exposes topic krabka-tv2-itest to admin client krabka-tv2-producer
    When the compiled Kafka 3.5 transactional helper runs under Kafka 4.3.1 jars
    Then the transactional helper reports TXNPROBE OK
    When the console reads 7 records using isolation read_committed
    Then the transactional records are exactly these values in log order
      | value       |
      | committed-0 |
      | committed-1 |
      | committed-2 |
      | committed-3 |
      | committed-4 |
      | committed-5 |
      | after-abort |
    When the console reads 9 records using isolation read_uncommitted
    Then the transactional records are exactly these values in log order
      | value       |
      | committed-0 |
      | committed-1 |
      | committed-2 |
      | committed-3 |
      | committed-4 |
      | committed-5 |
      | aborted-0   |
      | aborted-1   |
      | after-abort |
    When the administrator scrapes API counters and describes eos-tid and its partition producers
    Then AddPartitionsToTxn has zero requests and both coordinator and written producer epochs are at least 2

  Scenario: the transactions tool agrees with the broker and can abort
    Given a transaction.version 2 broker exposes topic krabka-tv2-open to admin client krabka-tv2-tool
    And an initialized producer holds tv2-open-tid open after flushing value open to krabka-tv2-open
    When the transactions tool and native administrator list, describe, inspect producers, and find hanging transactions
    Then the tools agree with native transaction identity, epoch, state, coordinator 1, partition krabka-tv2-open-0, and producer start offset
    And the ongoing coordinator-owned transaction is not reported as hanging
    When the operator aborts the open partition transaction through kafka-transactions
    Then the producer table reports transaction start offset None
    When the producer aborts its owned transaction and the transaction tool observes completion
    Then the transaction coordinator reports CompleteAbort
