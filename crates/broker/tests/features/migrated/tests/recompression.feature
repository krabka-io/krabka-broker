Feature: Topic compression policy and configured codec levels survive storage

  Scenario: topic compression lz4 recompresses producer gzip batch
    Given a topic recompress-target whose compression policy is lz4
    When a gzip producer appends payload broker-side recompression smoke and a consumer fetches the first batch
    Then the fetched batch uses lz4 compression and preserves the original payload

  Scenario: topic compression producer preserves producer gzip
    Given a topic passthrough whose compression policy is producer
    When a gzip producer appends payload passthrough payload and a consumer fetches the first batch
    Then the fetched batch uses gzip compression and preserves the original payload

  Scenario Outline: stored owned batches honor every codec level
    Given an owned record batch using <codec> at compression level <level>
    When the log appends the owned batch and reads its stored encoded bytes
    Then the stored batch exactly matches encoding at the configured compression level

    Examples:
      | codec | level |
      | gzip  | 1     |
      | gzip  | 9     |
      | lz4   | 9     |
      | lz4   | 17    |
      | zstd  | -5    |
      | zstd  | 12    |
