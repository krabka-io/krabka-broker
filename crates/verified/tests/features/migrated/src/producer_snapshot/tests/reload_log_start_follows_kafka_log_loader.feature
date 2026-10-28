Feature: Kafka producer-state snapshot reload
  Reload chooses local or remote log starts, keeps snapshots in the usable offset
  range, removes strays, and rejects invalid producer entries before replay.

  Scenario Outline: Remote and local logs establish different reload starting offsets
    Given remote storage is <remote> with checkpoint <checkpoint> and local segment start <local>
    When the producer-state loader chooses its log start
    Then producer-state reload starts at offset <start>

    Examples:
      | remote | checkpoint | local | start |
      | true | 3 | 8 | 3 |
      | true | none | 8 | 0 |
      | false | 3 | 8 | 8 |
      | false | none | 8 | 8 |

  Scenario Outline: Reload excludes the start boundary and includes the log end
    Given a producer reload range starting at 5 and ending at 10
    When the loader considers snapshot offset <offset>
    Then the snapshot is retained for reload: <kept>

    Examples:
      | boundary | offset | kept |
      | below start | 4 | false |
      | at start | 5 | false |
      | inside range | 6 | true |
      | at end | 10 | true |
      | above end | 11 | false |
      | negative | -1 | false |

  Scenario Outline: Stray removal preserves segment-aligned snapshots and the newest clean-shutdown snapshot
    Given snapshot offsets <snapshots> and segment bases <bases>
    When the loader classifies every snapshot as stray or retained
    Then the snapshot removal decisions are <decisions>

    Examples:
      | boundary | snapshots | bases | decisions |
      | all aligned | 0,4,8 | 0,4,8 | keep,keep,keep |
      | older strays | 2,4,6 | 0,4,8 | remove,keep,remove |
      | clean shutdown | 4,8,11 | 0,4,8 | keep,keep,keep |
      | newest above base | 9,11 | 0,4,8 | remove,keep |
      | no segments | 3,7 | empty | remove,keep |
      | unsorted inventory | 11,2,4 | 8,0,4 | keep,remove,keep |

  Scenario Outline: The newest usable snapshot wins and equal offsets keep the first entry
    Given a producer reload range starting at <start> and ending at <end>
    And available producer snapshot offsets <snapshots>
    When the loader selects the latest usable snapshot
    Then the selected snapshot index is <selected>

    Examples:
      | boundary | start | end | snapshots | selected |
      | no snapshots | 5 | 10 | empty | none |
      | all unusable | 5 | 10 | 3,5,11,-1 | none |
      | unsorted offsets | 5 | 10 | 6,10,7,12 | 1 |
      | start is excluded | 5 | 10 | 5,6 | 1 |
      | duplicate latest | 5 | 10 | 10,10 | 0 |
      | maximum offset | 9223372036854775806 | 9223372036854775807 | 9223372036854775807 | 0 |

  Scenario: Snapshot entry validation - real producer
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 9 | 3 | 0 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: true

  Scenario: Snapshot entry validation - marker only producer
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 0 | 0 | -1 | -1 | 0 | -1 | -1 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: true

  Scenario: Snapshot entry validation - integer extremes
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 9223372036854775807 | 9223372036854775807 | 32767 | 2147483647 | 9223372036854775806 | 2147483647 | 2147483647 | 9223372036854775805 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: true

  Scenario: Snapshot entry validation - negative snapshot offset
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | -1 | 0 | 0 | -1 | -1 | 0 | -1 | -1 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - negative producer id
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | -1 | 2 | 4 | 9 | 3 | 0 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - negative producer epoch
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | -1 | 4 | 9 | 3 | 0 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - coordinator below sentinel
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 9 | 3 | -2 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - last record at snapshot boundary
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 10 | 3 | 0 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - transaction at snapshot boundary
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 9 | 3 | 0 | 10 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - transaction below sentinel
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 9 | 3 | 0 | -2 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - delta before log start
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 0 | 1 | 0 | -1 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - marker sentinel with delta
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 0 | 0 | -1 | -1 | 1 | -1 | -1 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false

  Scenario: Snapshot entry validation - transaction after last record
    Given a producer snapshot contains this entry:
      | snapshot_offset | producer_id | producer_epoch | last_sequence | last_offset | offset_delta | coordinator_epoch | transaction_start |
      | 10 | 7 | 2 | 4 | 4 | 0 | 0 | 5 |
    When the producer snapshot entry is validated against its snapshot boundary
    Then the snapshot entry is valid: false
