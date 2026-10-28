Feature: Compression compatibility for legacy Fetch consumers
  Scenario: A legacy consumer receives a snappy rewrite of modern zstd records
    Given a modern topic holds fifty zstd-compressed records with distinct boundary keys
    When a Fetch v3 consumer reads the zstd-compressed modern batch
    Then the legacy wrapper advertises snappy compression codec two
    And all fifty records decode with boundary keys key-0000 and key-0049
