Feature: Raft snapshot fetch identity, page boundaries and leader hints

  Background: A completed controller snapshot
    Given an allow-all controller has a completed metadata snapshot and a captured first 16-byte page

  Scenario: The named snapshot starts with the captured page
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | position | 0 |
    Then snapshot fetch returns top-level code 0, partition code 0 and current leader present true
    And the snapshot response preserves the captured identity, total size, position zero and exact first-page bytes

  Scenario: Another end offset does not identify this snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | end offset delta | 1 |
    Then snapshot fetch returns top-level code 0, partition code 98 and current leader present true

  Scenario: Another snapshot epoch does not identify this snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | snapshot epoch delta | 1 |
    Then snapshot fetch returns top-level code 0, partition code 98 and current leader present true

  Scenario: Another topic has no metadata snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | topic | not-metadata |
    Then snapshot fetch returns top-level code 0, partition code 3 and current leader present false

  Scenario: Another partition has no metadata snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | partition | 1 |
    Then snapshot fetch returns top-level code 0, partition code 3 and current leader present false

  Scenario: Position at the end is outside the snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | position | snapshot size |
    Then snapshot fetch returns top-level code 0, partition code 99 and current leader present true

  Scenario: Negative position is outside the snapshot
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | position | -1 |
    Then snapshot fetch returns top-level code 0, partition code 99 and current leader present true

  Scenario: Stale leader epochs are fenced
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | leader epoch delta | -1 |
    Then snapshot fetch returns top-level code 0, partition code 74 and current leader present true

  Scenario: Future leader epochs are unknown
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | leader epoch delta | 1 |
    Then snapshot fetch returns top-level code 0, partition code 75 and current leader present true

  Scenario: Base64 cluster identity is accepted
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | cluster ID | base64 |
    Then snapshot fetch returns top-level code 0, partition code 0 and current leader present true
    And the snapshot response preserves the captured identity, total size, position zero and exact first-page bytes

  Scenario: Hyphenated cluster identity is accepted
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | cluster ID | hyphenated |
    Then snapshot fetch returns top-level code 0, partition code 0 and current leader present true
    And the snapshot response preserves the captured identity, total size, position zero and exact first-page bytes

  Scenario: Another cluster identity has no topic rows
    When FetchSnapshot reads the captured snapshot with these request fields
      | field | value |
      | cluster ID | another-cluster |
    Then snapshot fetch returns top-level code 104, partition code 0 and current leader present false
