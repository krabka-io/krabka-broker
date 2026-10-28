Feature: Record-preserving Fetch downconversion for legacy consumers
  Scenario: Fetch v3 preserves modern record keys and values in legacy framing
    Given a modern batch contains keyed records key0=val0 and key1=val1
    When Fetch version 3 requests the modern record batch
    Then the legacy message set preserves both record keys and values

  Scenario: Fetch v0 removes timestamps that its framing cannot represent
    Given a modern record has key k value v and a nonzero create timestamp
    When Fetch version 0 requests the modern record batch
    Then the v0 message set retains k and v but contains no record timestamp

  Scenario: Legacy conversion meters every converted partition under one topic
    Given three topic partitions each hold their own acknowledged record
    When Fetch v3 requests all three partitions together
    Then every returned partition contains its own legacy record and increments conversion metering
