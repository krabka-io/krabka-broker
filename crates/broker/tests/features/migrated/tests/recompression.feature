Feature: tests recompression

  Scenario: topic compression lz4 recompresses producer gzip batch
    Then recompression: topic compression lz4 recompresses producer gzip batch

  Scenario: topic compression producer preserves producer gzip
    Then recompression: topic compression producer preserves producer gzip

  Scenario: stored owned batches honor every codec level
    Then recompression: stored owned batches honor every codec level
