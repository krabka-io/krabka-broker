Feature: Metadata snapshot pages identify their snapshot and bounds

  Scenario: fetch snapshot serves the named metadata snapshot
    Given a broker with an owned metadata data directory
    And the metadata image contains topic snap-topic
    When a controller snapshot is requested
    And FetchSnapshot requests topic __cluster_metadata, snapshot offset 0, snapshot epoch 0, leader epoch 0, and position 0
    Then the partition refuses stale epoch with code 74 and advertises leader 1 with a positive epoch
    When the checkpoint file appears within 30 seconds and its first page is requested using the advertised epoch
    Then the exact named snapshot succeeds on partition zero with a positive total size and nonempty page
    When the same epoch requests a snapshot with offset increased by 1 at position zero
    Then FetchSnapshot has top-level error 0 and partition error 98
    When the named snapshot is requested at its reported total size
    Then FetchSnapshot has top-level error 0 and partition error 99

  Scenario: fetch snapshot rejects non metadata topic
    Given a broker with an owned metadata data directory
    When FetchSnapshot requests topic not-metadata, snapshot offset 0, snapshot epoch 0, leader epoch 0, and position 0
    Then FetchSnapshot has top-level error 0 and partition error 3
