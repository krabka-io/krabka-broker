Feature: Streams member reconciliation uses explicit task ownership and target epochs
  Scenario: 1 a stable member takes the new tasks at the target epoch
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [0] | [] | [] | [] |
    And m1 target active tasks are [0,1] standby tasks [] and heartbeat owns active [0] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | Stable | 2 | [0,1] | [] | [] | [] |
  Scenario: 2 a member keeps its epoch while it owns a revoked task
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [0,1] | [] | [] | [] |
    And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0,1] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
  Scenario: 3 a member that reports no tasks has nothing to revoke
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [0,1] | [] | [] | [] |
    And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | Stable | 2 | [0] | [] | [] | [] |
  Scenario: 4 an unrevoked member moves on once it stops reporting the task
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
    And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | Stable | 2 | [0] | [] | [] | [] |
  Scenario: 5 an unrevoked member that still reports the task waits
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
    And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0,1] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | false | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
  Scenario: 6 an unrevoked member without owned tasks waits
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
    And m1 target active tasks are [0] standby tasks [] and heartbeat owns active null standby null
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | false | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
  Scenario: 7 an active task that another member still owns is held back
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
      | m2 | p2 | UnrevokedTasks | 1 | [] | [] | [1] | [] |
    And m1 target active tasks are [0,1] standby tasks [] and heartbeat owns active [] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | UnreleasedTasks | 2 | [0] | [] | [] | [] |
  Scenario: 8 a standby task that the same process runs as active is held back
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
      | m2 | p1 | Stable | 1 | [0] | [] | [] | [] |
    And m1 target active tasks are [] standby tasks [0] and heartbeat owns active [] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | UnreleasedTasks | 2 | [] | [] | [] | [] |
  Scenario: 9 a standby task that another process runs as active is given
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
      | m2 | p2 | Stable | 1 | [0] | [] | [] | [] |
    And m1 target active tasks are [] standby tasks [0] and heartbeat owns active [] standby []
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | Stable | 2 | [] | [0] | [] | [] |
  Scenario: 10 a revoked standby task waits for its release
    Given Streams group g at epoch 2 has these members in subtopology s
      | member | process | state | epoch | active | standby | active_pending | standby_pending |
      | m1 | p1 | Stable | 1 | [] | [0] | [] | [] |
    And m1 target active tasks are [] standby tasks [] and heartbeat owns active [] standby [0]
    When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
    Then m1 has this complete reconciliation result
      | changed | state | epoch | active | standby | active_pending | standby_pending |
      | true | UnrevokedTasks | 1 | [] | [] | [] | [0] |
