Feature: tests replication idempotent failover

  Scenario: a leader elected by a controlled shutdown answers a replicated retry as a duplicate
    Then replication idempotent failover: a leader elected by a controlled shutdown answers a replicated retry as a duplicate

  Scenario: a leader elected again continues from the batches it replicated
    Then replication idempotent failover: a leader elected again continues from the batches it replicated
