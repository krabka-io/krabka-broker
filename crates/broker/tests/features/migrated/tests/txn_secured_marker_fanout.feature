Feature: tests txn secured marker fanout

  Scenario: end txn marker fanout to remote leader over sasl
    Given two SASL_PLAINTEXT brokers lead one partition each and tid remote-fanout-tid registers the partition led away from its coordinator
    When EndTxn commits remote-fanout-tid through its authenticated coordinator
    Then EndTxn returns NONE after the authenticated remote marker fan-out

  Scenario: a remote coordinator verifies a produce for a broker with only cluster action
    Given two SASL brokers grant broker only ClusterAction on kafka-cluster and client Write on all transactional IDs and topics
    When client produces one transactional v record at sequence 0 and timestamp 1 through a remote partition leader and commits each transaction
      | name                    | produce_version |
      | verify-only-produce-v11  | 11              |
      | add-produce-v12          | 12              |
    Then every complete Produce partition response succeeds at base offset 0, log append time -1 and log start 0, and EndTxn returns 0
