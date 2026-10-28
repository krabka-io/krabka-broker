Feature: Kafka cleaner retention of emptied batch headers
  Background:
    Given a compaction pass at time 200 with delete retention 50 milliseconds

  Scenario: The active producer retains the header of its last data batch
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | data | 1000 | 7 | none |
      | 1 | data | 1000 | 7 | none |
      | 2 | data | -1 | -1 | k |
    And producer 1000 last data offset 1 is active at epoch 7
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 1,2

  Scenario: Inactive producers retain no obsolete data headers
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | data | 1000 | 7 | none |
      | 1 | data | 1000 | 7 | none |
      | 2 | data | -1 | -1 | k |
    And producer 1000 is inactive
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 2

  Scenario: A current-epoch marker retains a marker-only producer
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | expired-commit | 1000 | 7 | control |
      | 1 | data | -1 | -1 | k |
    And producer 1000 last data offset none is active at epoch 7
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 0,1

  Scenario: A marker from an older producer epoch is discarded
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | expired-commit | 1000 | 7 | control |
      | 1 | data | -1 | -1 | k |
    And producer 1000 last data offset none is active at epoch 8
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 1

  Scenario: A marker-only producer does not retain a discarded data header
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | data | 1000 | 7 | none |
      | 1 | data | -1 | -1 | k |
    And producer 1000 last data offset none is active at epoch 7
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 1

  Scenario: Only the last anonymous batch can retain an empty header
    Given compaction input batches
      | offset | kind | producer | epoch | key |
      | 0 | data | -1 | -1 | k |
      | 1 | data | -1 | -1 | none |
      | 2 | data | -1 | -1 | none |
    And producer 1000 is inactive
    When the cleaner rewrites the compaction segment
    Then the surviving batch headers have base offsets 0,2
