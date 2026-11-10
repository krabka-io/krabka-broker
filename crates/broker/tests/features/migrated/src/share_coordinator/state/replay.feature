Feature: Share snapshot and update replay

  Rule: An update replaces the offsets, epochs and delivery ranges of a replayed snapshot and keeps its metadata

    Scenario: An update advances a replayed snapshot without losing metadata
      Given a replay snapshot has snapshot epoch 1, state epoch 2, leader epoch 3, and record offset 7
      And the replay snapshot starts at 0 with completed count 0, creation time 10, and write time 20
      And the replay snapshot delivery ranges are:
        | first | last | delivery  | attempts |
        | 0     | 9    | available | 1        |
        | 10    | 19   | available | 1        |
        | 20    | 29   | available | 1        |
      And the replay update has snapshot epoch 1, leader epoch 4, start offset 20, and completed count 7
      And the replay update delivery ranges are:
        | first | last | delivery  | attempts |
        | 30    | 39   | available | 1        |
      When the coordinator replays the snapshot followed by the update
      Then the complete replayed share state is:
        | field                     | value                     |
        | state_epoch               | 2                         |
        | leader_epoch              | 4                         |
        | start_offset              | 20                        |
        | delivery_complete_count   | 7                         |
        | state_batches             | [[20,39,"available",1]]   |
        | snapshot_epoch            | 1                         |
        | create_timestamp          | 10                        |
        | write_timestamp           | 20                        |
        | last_snapshot_offset      | 7                         |
        | updates_since_snapshot    | 1                         |
        | fence_leader_epoch        | 4                         |
        | fence_state_epoch         | 2                         |

  Rule: An update with start offset -1 and leader epoch -1 keeps the stored start offset and leader epoch

    Scenario: An update that leaves both unset keeps them and clips its ranges at the kept start
      Given a replay snapshot starts at offset 50 with leader epoch 3
      And a replay update has start offset -1, leader epoch -1 and available offsets 40 to 55
      When the coordinator replays the snapshot followed by the update
      Then the complete replayed share state is:
        | field                     | value                     |
        | state_epoch               | 0                         |
        | leader_epoch              | 3                         |
        | start_offset              | 50                        |
        | delivery_complete_count   | 0                         |
        | state_batches             | [[50,55,"available",1]]   |
        | snapshot_epoch            | 0                         |
        | create_timestamp          | 0                         |
        | write_timestamp           | 0                         |
        | last_snapshot_offset      | 0                         |
        | updates_since_snapshot    | 1                         |
        | fence_leader_epoch        | 3                         |
        | fence_state_epoch         | 0                         |
