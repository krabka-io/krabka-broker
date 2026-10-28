Feature: Soak contract execution plans enforce storage semantics
  Scenario Outline: tests: every checked in contract is runnable
    Given soak contract documents <file> are loaded
    When the executor checks whether each soak contract can run
    Then the soak executor reports no unsupported contract requirements

    Examples:
      | example    | file            |
      | self check | self-check.yaml |
      | soak       | rf3-72h.yaml    |

  Scenario: tests: a drill is left to the qualification drill
    Given soak contract documents dr-secured.yaml are loaded
    When the executor checks whether each soak contract can run
    Then the disaster-recovery contract is directed to its secured qualification drill

  Scenario: tests: a compacted tiered path is refused
    Given the self-check contract adds compacted tiered-idempotent with key space eight
    When the executor checks whether each soak contract can run
    Then the compacted tiered path is refused because Kafka forbids remote storage on compacted topics

  Scenario Outline: tests: only a diskless contract runs a wal quorum
    Given soak contract documents <file> are loaded
    When the executor identifies which contracts require a diskless WAL quorum
    Then a WAL quorum is required: <quorum>

    Examples:
      | example | file            | quorum |
      | classic | self-check.yaml | false  |
      | soak    | rf3-72h.yaml    | true   |

  Scenario: An idempotent classic path keeps an unbounded delete-policy topic
    Given the self-check idempotent path uses classic storage
    When the executor renders topic configuration for the selected load path
    Then the complete ordered topic configuration is:
      | name                 | value  |
      | min.insync.replicas  | 2      |
      | cleanup.policy       | delete |
      | retention.ms         | -1     |
      | retention.bytes      | -1     |

  Scenario: A compacted classic path enables frequent cleaner opportunities
    Given the self-check compacted path uses classic storage
    When the executor renders topic configuration for the selected load path
    Then the complete ordered topic configuration is:
      | name                        | value   |
      | min.insync.replicas         | 2       |
      | cleanup.policy              | compact |
      | internal.segment.bytes      | 65536   |
      | segment.ms                  | 5000    |
      | min.cleanable.dirty.ratio    | 0.01    |
      | min.compaction.lag.ms        | 0       |

  Scenario: A tiered idempotent path retains sealed data remotely
    Given the self-check idempotent path uses tiered storage
    When the executor renders topic configuration for the selected load path
    Then the complete ordered topic configuration is:
      | name                    | value   |
      | min.insync.replicas     | 2       |
      | cleanup.policy          | delete  |
      | retention.ms            | -1      |
      | retention.bytes         | -1      |
      | remote.storage.enable   | true    |
      | internal.segment.bytes  | 4194304 |
      | segment.ms              | 15000   |
      | local.retention.bytes   | 1       |

  Scenario: A diskless idempotent path selects the diskless log
    Given the self-check idempotent path uses diskless storage
    When the executor renders topic configuration for the selected load path
    Then the complete ordered topic configuration is:
      | name                 | value  |
      | min.insync.replicas  | 2      |
      | cleanup.policy       | delete |
      | retention.ms         | -1     |
      | retention.bytes      | -1     |
      | krabka.diskless      | true   |
