Feature: Transactional isolation across sealed log segments
  Scenario: transactional console producer eos
    Given three brokers expose external and internal listeners with 128-byte segments and topic krabka-txn-itest
    When the JVM helper commits six records, aborts two, and appends after-abort
    Then the JVM exits successfully and reports TXNPROBE OK
    When a console consumer reads 7 records with read_committed through broker 3
    Then the console returns exactly these values in log order
      | value       |
      | committed-0 |
      | committed-1 |
      | committed-2 |
      | committed-3 |
      | committed-4 |
      | committed-5 |
      | after-abort |
    When a console consumer reads 9 records with read_uncommitted through broker 3
    Then the console returns exactly these values in log order
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
    When two JVM producers claim zombie-tid on topic krabka-txn-zombie and the superseded producer writes
    Then the JVM exits successfully and reports ZOMBIEPROBE fenced=ProducerFencedException
