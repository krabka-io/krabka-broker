Feature: Producer batch validation before appending
  Scenario: Clients cannot append broker-only transaction control batches
    Given a client authored a control batch containing one transaction marker-shaped record
    When the client submits this candidate producer batch
    Then the batch is refused as INVALID_RECORD 87 without appending anything

  Scenario: A valid checksum cannot make an invalid record body appendable
    Given a producer batch has a zero-length record body but a recomputed valid CRC
    When the client submits this candidate producer batch
    Then the batch is refused as INVALID_RECORD 87 without appending anything
