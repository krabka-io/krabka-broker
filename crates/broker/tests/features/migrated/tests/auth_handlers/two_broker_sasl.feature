Feature: Replication over authenticated inter-broker connections

  Scenario: two broker sasl plaintext replication
    Given two registered brokers authenticate inter-broker fetch and heartbeat with PLAIN
    And topic sasl-repl has replicas 1 and 2 with broker 1 as leader
    When 10 records are produced to broker 1 over the data-plane listener
    Then the replicated produce is acknowledged without error
    And both authenticated replicas reach log end offset 10
