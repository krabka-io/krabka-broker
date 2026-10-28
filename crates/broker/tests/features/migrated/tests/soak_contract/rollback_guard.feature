Feature: Rollback distinguishes durable mutation from normal index rebuilding
  Scenario: An untouched segment has no changed files
    Given rollback files before opening are:
      | name  | contents_hex |
      | 0.log | 6162         |
    And rollback files after opening are:
      | name  | contents_hex |
      | 0.log | 6162         |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are none and derived rebuilds are none

  Scenario: A removed producer snapshot is a derived rebuild
    Given rollback files before opening are:
      | name        | contents_hex |
      | 0.log       | 6162         |
      | 55.snapshot | 73           |
    And rollback files after opening are:
      | name  | contents_hex |
      | 0.log | 6162         |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are none and derived rebuilds are 55.snapshot

  Scenario: A rebuilt time index is derived
    Given rollback files before opening are:
      | name        | contents_hex |
      | 0.timeindex |              |
    And rollback files after opening are:
      | name        | contents_hex |
      | 0.timeindex | 656e747279   |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are none and derived rebuilds are 0.timeindex

  Scenario: Trimming unused offset-index bytes is derived
    Given rollback files before opening are:
      | name    | contents_hex |
      | 0.index | 616200       |
    And rollback files after opening are:
      | name    | contents_hex |
      | 0.index | 6162         |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are none and derived rebuilds are 0.index

  Scenario: Rewriting a durable checkpoint is a persisted change
    Given rollback files before opening are:
      | name           | contents_hex |
      | 0-0.checkpoint | 6162         |
    And rollback files after opening are:
      | name           | contents_hex |
      | 0-0.checkpoint | 6163         |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are 0-0.checkpoint and derived rebuilds are none

  Scenario: Appending to a durable segment is a persisted change
    Given rollback files before opening are:
      | name  | contents_hex |
      | 0.log | 6162         |
    And rollback files after opening are:
      | name  | contents_hex |
      | 0.log | 616200       |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are 0.log and derived rebuilds are none

  Scenario: Writing quorum-state is a persisted change
    Given rollback files before opening are:
      | name | contents_hex |
    And rollback files after opening are:
      | name         | contents_hex |
      | quorum-state | 7b7d         |
    When the rollback guard compares persisted and rebuildable file changes
    Then persisted changes are quorum-state and derived rebuilds are none

  Scenario: tests: a directory is read file by file
    Given a rollback data directory contains p-0/0.log with byte x
    When the rollback guard reads each file's bytes below the data directory
    Then the byte image contains only p-0/0.log with x

  Scenario: tests: the refusal is kafkas with the sources range
    Given the rollback source supports feature levels seven through thirty
    When the rollback guard explains refusal of metadata.version thirty-three
    Then the refusal uses Kafka's FeatureLevelRecord wording and the source range
