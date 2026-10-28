Feature: Batched diskless writes, cold reads and follower catch-up

  Scenario: the krabka consumer reads every batched record from earliest
    Given a three-partition diskless topic with 50 millisecond flushing and 512 byte batched payloads
    When the idempotent producer writes 600 records per partition in bursts of 60 and closes
    Then the batching producer acknowledges exactly 1800 records
    When every leader trims its first batch and the earliest-offset consumer reads through each high watermark
    Then all 1800 consumed offsets are contiguous from zero through each partition's recorded end
    When every nonleader WAL voter catches up to each partition's recorded high watermark within one minute
    Then every nonleader durable checkpoint covers its partition's recorded end
    And the batched diskless cluster is shut down

  Scenario: a follower restarted under load rejoins the isr
    Given a three-partition diskless topic with 50 millisecond flushing and 512 byte batched payloads
    When continuous batched load spans a follower stop, ISR removal and restart with three-second settling periods
    Then every partition's ISR contains all 3 voters after the restart
    And the batched diskless cluster is shut down
