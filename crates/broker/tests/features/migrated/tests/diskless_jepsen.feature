Feature: Acknowledged diskless history survives overlapping authority, WAL and object-store faults

  Scenario: three broker fault schedule preserves the acked ledger
    Given a three-voter RF1 diskless topic whose sole replica is the controller leader and whose object PUT namespace is blocked
    When two concurrent idempotent appenders each produce four records with acks-all
    Then the acknowledged 8 record ledger is gap-free, unique and linearizable in real-time order
    When the blocked flusher fails a PUT and both nonreplica voters checkpoint the acknowledged prefix
    And the accepting controller is crashed and its canonical log erased before a surviving controller and partition leader take over
    Then both authority leaders differ from the erased WAL node and object PUTs are still blocked
    When the Rust partition reader fetches the acknowledged ledger from a survivor while object PUTs remain blocked
    Then the Rust offsets and payload bytes exactly equal the acknowledged ledger
    When the replacement flusher fails another PUT before the object namespace is unblocked and a WAL object is written
    Then the replacement flusher's failure counter advanced and the object namespace is a directory
    When the official JVM console consumer drains the acknowledged ledger through a survivor
    Then the JVM consumer succeeds with exactly the acknowledged payload bytes in offset order, one per line
    And the diskless fault-schedule cluster is shut down
