Feature: Compression compatibility for legacy Fetch consumers
  Scenario: A legacy consumer gets UNSUPPORTED_COMPRESSION_TYPE for a partition of zstd records
    Given a modern topic holds fifty zstd-compressed records
    When a Fetch v3 consumer reads the zstd-compressed partition from its start
    Then the partition reports UNSUPPORTED_COMPRESSION_TYPE with an empty record set

  Scenario: A legacy consumer receives the records written before the first zstd batch
    Given a modern topic holds an uncompressed record followed by fifty zstd-compressed records
    When a Fetch v3 consumer reads the mixed partition from its start
    Then the partition succeeds with only the uncompressed record
